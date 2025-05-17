capture log close
log using "C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\src\ado\strl.log", replace

local path C:\Users\jonro\Downloads
local file sample_strl_test


pq describe using "`path'/`file'.parquet"
pq use using "`path'/`file'.parquet", clear

replace long_string = "hello" if _n == 1
replace long_string = "A" + char(0) + "B" + char(255) + char(1) + "test" if _n == 2
pq save "`path'/`file'_write.parquet", replace


pq use using "`path'/`file'_write.parquet", clear

capture log close