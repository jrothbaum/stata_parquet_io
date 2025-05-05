use std::collections::HashMap;
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



#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub dtype: String,
    pub stata_type: String,
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

    let json_string = serde_json::to_string(&all_columns).unwrap();
    
    //  Set macros for stata to create the empty data set

    //      The serialized mapping information
    let _ = set_macro(
        "mapping",
        &json_string,
        false
    );


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