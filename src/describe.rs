use polars::prelude::*;
use polars_sql::SQLContext;


use crate::mapping::schema_with_stata_types;
use crate::stata_interface:: {
    ST_retcode,
    display,
    set_macro,
};

use crate::read::{
    cast_catenum_to_string, 
    scan_lazyframe    
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
) -> i32 {
    
    let mut df = match scan_lazyframe(
        &path,
        safe_relaxed,
        asterisk_to_variable_name,
    ) {
        Ok(df) => df,
        Err(e) => {
            display(&format!("Error scanning lazyframe: {:?}", e));
            return 198
        },
    };

    //  Set cast macro to empty
    set_macro(
        &"cast_json",
        &"",
        false
    );
    
    if compress | compress_string_to_numeric {
        let mut downcast_config = DowncastConfig::default();
        downcast_config.check_strings = compress_string_to_numeric;
        downcast_config.prefer_int_over_float = compress;
        
        df = match intelligent_downcast(
            df,
            None,
            downcast_config
        ) {
            Ok(lf) => lf,
            Err(e) => {
                display("Error on compress");
                return 198;
            }
        }
    }
    let schema = match df.collect_schema() {
        Ok(schema) => schema,
        Err(e) => {
            display(&format!("Error collecting schema: {:?}", e));
            return 198
        },
    };
    
    //  display(&format!("schema: {:?}", schema));
    if let Some(sql) = sql_if.filter(|s| !s.trim().is_empty()) {
        let mut ctx = SQLContext::new();
        ctx.register("df", df);
        


        df = match ctx.execute(&format!("select * from df where {}",sql)) {
            Ok(lazyframe) => lazyframe,
            Err(e) => {
                display(&format!("Error in SQL if statement: {}", e));
                return 198 as ST_retcode;
            }
        };
    }

    df = cast_catenum_to_string(&df).unwrap();

    schema_with_stata_types(
        &df,
        &schema,
        quietly,
        detailed
    );

    
    let n_vars = schema.len();
    let n_rows = get_row_count(&df).unwrap();
    
    //  Return scalars of the number of columns and rows 
    let _ = set_macro("n_columns", &(format!("{}",n_vars)), false);
    let _ = set_macro("n_rows", &(format!("{}",n_rows)),false);

    if !quietly {
        display(&"");
        display(&format!("n columns = {}", n_vars));
        display(&format!("n rows = {}", n_rows));
    }

    return 0 as ST_retcode;
} 

pub fn get_schema(path:&str) -> PolarsResult<Schema> {
    let mut scan_args = ScanArgsParquet::default();
    scan_args.allow_missing_columns = true;
    scan_args.cache = false;
    let mut df = LazyFrame::scan_parquet(path, scan_args.clone())?;

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

