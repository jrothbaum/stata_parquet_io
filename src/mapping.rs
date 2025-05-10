use std::collections::{
    HashMap,
    HashSet
};
use serde::{Serialize, Deserialize};
use polars::prelude::*;

use crate::stata_interface::{
    display,
    set_macro,
    set_scalar
};
use serde_json;


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
        }
    }
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
        DataType::Int8 => StataType::Byte,
        DataType::Int16 => StataType::Int,
        DataType::Int32 => StataType::Long,
        DataType::Int64 => StataType::Double, // Only double can contain the set of possible values
        DataType::UInt8 => StataType::Byte, 
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
        DataType::String => {
            if str_length > 2045 {
                StataType::Strl
            } else {
                StataType::String
            }
        },
        
        // Other types default to Double (most flexible numeric type)
        _ => StataType::Double,
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
        StataType::String | StataType::Strl => DataType::String
    }
}



#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnInfo {
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
}

pub fn stata_column_info_to_schema(
    column_info: &Vec<StataColumnInfo>
) -> Schema {
    let fields: Vec<Field> = column_info.iter().map(|col| {
        // Parse the stata_type string to StataType enum
        
        let stata_type = match (col.dtype.to_lowercase().as_ref(),&col.format) {
            ("string",_) => StataType::String,
            ("strl",_) => StataType::Strl,
            ("int",_) => {
                let date_type = match_var_format_stata(&col.format);
                match date_type {
                    None => StataType::Int,
                    _ => date_type.unwrap()
                }
            },
            ("long",_) => {
                let date_type = match_var_format_stata(&col.format);
                match date_type {
                    None => StataType::Long,
                    _ => date_type.unwrap()
                }
            },
            ("float",_) => {
                let date_type = match_var_format_stata(&col.format);
                match date_type {
                    None => StataType::Float,
                    _ => date_type.unwrap()
                }
            },
            ("double",_) => {
                let date_type = match_var_format_stata(&col.format);
                match date_type {
                    None => StataType::Double,
                    _ => date_type.unwrap()
                }
            },
            (_,_) => panic!("Unknown Stata type: {}", &col.dtype),
        };
        
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
    detailed:bool
) {

    if !quietly {
        display(&String::from("Variable Name                    | Polars Type                      | Stata Type"));
        display(&String::from("-------------------------------- | -------------------------------- | -------------------- "));
    }

    let hash_strings = if detailed {
            //  Get length of string columns and assign to hashmap
            get_string_column_lengths(&df, &schema).unwrap()
        } else {
            HashMap::<PlSmallStr, usize>::new()
        };

    let rename_map = generate_rename_map(&schema);
    let mut all_columns:Vec<ColumnInfo> = Vec::with_capacity(schema.len());
    for (i,(name, dtype)) in schema.iter().enumerate() {
        let char_length = hash_strings.get(name).unwrap_or(&0);
        let stata_type = map_polars_to_stata(dtype,*char_length);

        let column_info = ColumnInfo {
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
    

    //  let json_string = serde_json::to_string(&all_columns).unwrap();
    
    //  Set macros for stata to create the empty data set

    //      The serialized mapping information
    // let _ = set_macro(
    //     "mapping",
    //     &json_string,
    //     false
    // );


}



fn generate_rename_map(schema: &Schema) -> HashMap<String, String> {
    let mut rename_map: HashMap<String, String> = HashMap::new();
    let mut used_names: HashSet<String> = HashSet::new();

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
    
    // Third pass: Resolve collisions and build final rename map
    let mut used_final_names: HashSet<String> = HashSet::new();
    
    for (original_name, processed_name) in processed_names {
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
            if matches!(dtype, DataType::String) {
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
            col(col_name.as_str()).str().len_chars().max().alias(col_name.as_str())
        })
        .collect();
    

    // Execute the query and get the result DataFrame
    let result_df = df.clone().select(exprs).collect()?;

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