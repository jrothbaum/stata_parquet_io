*! pq_save: Save data to parquet files - see polars_parquet.ado and polars_parquet.sthelp for details
program define pq_save
    version 17.0
    
    // Call the actual implementation
    _pq_save `0'
end