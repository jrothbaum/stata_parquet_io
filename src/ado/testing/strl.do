local path C:\Users\jonro\Downloads
local file sample_strl_test


pq describe using "`path'/`file'.parquet"
pq use using "`path'/`file'.parquet", clear

replace long_string = "hello" if _n == 1
replace long_string = "A_B_test" if _n == 2
pq save "`path'/`file'_write.parquet", replace


pq use using "`path'/`file'_write.parquet", clear
assert long_string[1] == "hello"
assert long_string[2] == "A_B_test"
list
