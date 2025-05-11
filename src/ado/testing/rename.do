local path C:\Users\jonro\Downloads
local file sample_variable_rename_test


pq describe using "`path'/`file'.parquet"

pq use using "`path'/`file'.parquet", clear
return list


pq save using "`path'/`file'_saved.parquet", replace noautorename
pq describe using "`path'/`file'_saved.parquet"


pq save using "`path'/`file'_saved.parquet", replace
pq describe using "`path'/`file'_saved.parquet"