*! pq_use: Read data from parquet files - see polars_parquet.ado and polars_parquet.sthelp for details
program define pq_use
    version 17.0
    
    // Call the actual implementation
    _pq_use `0'
end

// Ensure main package is loaded once
capture confirm program pq_register_plugin
if _rc {
    // Load package only if not already loaded
    if ("${parquet_path_override}" != "") {
        do "${parquet_path_override}\polars_parquet.ado"
    }
    else {
        do "`c(sysdir_plus)'\p\polars_parquet.ado"
    }
    pq_register_plugin
}