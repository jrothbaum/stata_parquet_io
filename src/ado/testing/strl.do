local path C:\Users\jonro\Downloads
local file sample_strl_test


pq describe using "`path'/`file'.parquet"
pq use using "`path'/`file'.parquet", clear


