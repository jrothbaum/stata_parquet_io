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
		quietly gen long c_even_longer_var_name_`cols_created' = _n
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_even_longer_var_name_`cols_created' = char(65 + floor(runiform()*5))
	}
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_even_longer_var_name_`cols_created' = rnormal()
		quietly tostring c_even_longer_var_name_`cols_created', replace force
	}
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_even_longer_var_name_`cols_created' = floor(runiform()*100)
	}
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_even_longer_var_name_`cols_created' = 100 + floor(runiform()*60)
	}
	
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_even_longer_var_name_`ci' = `"My values is "`n_cols'", yep"'
			label variable c_even_longer_var_name_`ci' "V1 stayer, \`87 stayer if same ein in 85-86'"
		}
	}
end

set seed 1000

local n_rows = 100
local n_cols = 20

create_data, n_rows(`n_rows') n_cols(`n_cols') 
tempfile tparquet
local tparquet `tparquet'_using_a_thing
compress

pq save "`tparquet'.parquet", replace

pq use "`tparquet'.parquet", clear

assert _N == `n_rows'