set varabbrev off
set more off

tempfile trename

clear
set obs 5
gen long x = _n
gen str8 y = "row_" + string(_n)

label variable x "{parquet_name:original_x}"
label variable y "{parquet_name:original_y}"

pq save using "`trename'_auto.parquet", replace
pq describe using "`trename'_auto.parquet", quietly
assert real("`r(n_columns)'") == 2
assert "`r(name_1)'" == "original_x"
assert "`r(name_2)'" == "original_y"

pq use using "`trename'_auto.parquet", clear
assert _N == 5
capture confirm variable original_x
assert _rc == 0
capture confirm variable original_y
assert _rc == 0

clear
set obs 5
gen long x = _n
gen str8 y = "row_" + string(_n)
label variable x "{parquet_name:original_x}"
label variable y "{parquet_name:original_y}"

pq save using "`trename'_noauto.parquet", replace noautorename
pq describe using "`trename'_noauto.parquet", quietly
assert real("`r(n_columns)'") == 2
assert "`r(name_1)'" == "x"
assert "`r(name_2)'" == "y"

pq use using "`trename'_noauto.parquet", clear
assert _N == 5
capture confirm variable x
assert _rc == 0
capture confirm variable y
assert _rc == 0

capture erase "`trename'_auto.parquet"
capture erase "`trename'_noauto.parquet"

di as result "rename.do: auto/noautorename tests PASSED"
