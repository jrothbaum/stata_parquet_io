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
create_data, n_rows(100) n_cols(1000) 

compress
tempfile tparquet

save "`tparquet'", replace
pq save "`tparquet'.parquet", replace
pq use "`tparquet'.parquet", clear

quietly describe
local n_k = r(k)
local n = _N
assert `n_k' == 1000
assert `n' == 100
pq use c_1*0 using "`tparquet'.parquet", clear
quietly describe
local n_k = r(k)
local n = _N

assert `n_k' == 12
assert `n' == 100

local varlist
forvalues i = 1/100 {
	local varlist `varlist' c_`i'
}

pq use `varlist' using "`tparquet'.parquet", clear
quietly describe
local n_k = r(k)
local n = _N

assert `n_k' == 100
assert `n' == 100



local n_to_load = 900
local varlist
forvalues i = 1/`n_to_load'{
	local varlist `varlist' c_`i'
}

use `varlist' using "`tparquet'", clear
quietly describe
local n_k = r(k)
local n = _N

assert `n_k' == `n_to_load'
assert `n' == 100

local varlist
forvalues i = 1/`n_to_load' {
	local varlist `varlist' c_`i'
}

pq use `varlist' using "`tparquet'.parquet", clear
quietly describe
local n_k = r(k)
local n = _N

assert `n_k' == `n_to_load'
assert `n' == 100

