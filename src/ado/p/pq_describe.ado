*! pq_describe: Get parquet file information - see polars_parquet.ado and polars_parquet.sthelp for details 
program define pq_describe
    version 17.0

    // Call the actual implementation
    _pq_describe `0'
end