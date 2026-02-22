set varabbrev off

capture program drop create_data
program define create_data
	version 16
	syntax		, 	n_cols(integer)			///
					n_rows(integer)
	
	clear
	set obs `n_rows'
	local cols_created = 0

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen long c_`cols_created' = _n
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = char(65 + floor(runiform()*5))
	}
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = rnormal()
		quietly tostring c_`cols_created', replace force
	}
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = floor(runiform()*100)
	}
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = 100 + floor(runiform()*60)
	}
	
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_`ci' = rnormal()
		}
	}
end

set seed 1000
di "Parallelization"
create_data, n_rows(100000) n_cols(10) 
tempfile tparquet
compress
pq save "`tparquet'.parquet", replace

clear
timer clear
timer on 1
pq use "`tparquet'.parquet", clear parallelize(columns) compress
timer off 1
assert _N == 100000

clear
timer on 2
pq use "`tparquet'.parquet", clear parallelize(rows)
timer off 2
assert _N == 100000

di "1:	Columns"
di "2:	Rows"
timer list

pq use * using "`tparquet'.parquet", clear
assert _N == 100000
confirm variable c_1
confirm variable c_10
sum
pq use c_* using "`tparquet'.parquet", clear
assert _N == 100000
confirm variable c_1
confirm variable c_10
pq use c_1* c_4 using "`tparquet'.parquet", clear
assert _N == 100000
confirm variable c_1
confirm variable c_10
confirm variable c_4
capture confirm variable c_2
assert _rc != 0

sum


pq use "`tparquet'.parquet", clear sort(c_2 c_1)
forvalues i=1/10 {
	di c_1[`i']
	di c_2[`i']
}
assert c_2[1] == "A"

pq use "`tparquet'.parquet", clear sort(-c_2 -c_1)
forvalues i=1/10 {
	di c_1[`i']
	di c_2[`i']
}
assert c_2[1] == "E"



pq use * using "`tparquet'.parquet", clear compress
assert _N == 100000
local c4type: type c_4
assert "`c4type'" == "byte"
describe
sum

capture erase `tparquet'.parquet






di "Asterisk as variable name"
create_data, n_rows(100) n_cols(10) 
gen year_match = 2018
pq save "`tparquet'_2018.parquet", replace
replace year_match = 2019
recast str100 c_2 
gen additional_var = _n
pq save "`tparquet'_2019.parquet", replace

clear

pq describe "`tparquet'_*.parquet", asterisk_to_variable(year)
return list
pq use "`tparquet'_*.parquet", clear asterisk_to_variable(year)
assert _N == 200
confirm variable year

sum
pq use "`tparquet'_2018.parquet", clear
assert _N == 100
sum
describe
pq append "`tparquet'_2019.parquet", compress
assert _N == 200
sum
describe


clear

create_data, n_rows(100) n_cols(10) 
forvalues i = 2/10 {
	rename c_`i' c_`=`i'+10'
}
pq save "`tparquet'_merge.parquet", replace

pq use "`tparquet'_2018.parquet", clear
pq merge 1:1 c_1 using "`tparquet'_merge.parquet"
assert _N == 100

pq use "`tparquet'_2018.parquet", clear
pq merge 1:1 _n using "`tparquet'_merge.parquet", compress
assert _N == 100



capture erase `tparquet'_2018.parquet
capture erase `tparquet'_2019.parquet

clear