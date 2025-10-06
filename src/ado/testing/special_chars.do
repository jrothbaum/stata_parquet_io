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


create_data, n_rows(10000) n_cols(10) 
tempfile tparquet
compress

di c_2[1]
describe

replace c_2 = "Consultoria, Científico, Técnico" if _n <=1000
di c_2[1]
pq save "`tparquet'.parquet", replace
clear

pq describe using "`tparquet'.parquet", detailed

pq use "`tparquet'.parquet", clear

di c_2[1]
replace c_2 = "Consultoria, Científico, Tecnico__" if _n <=1000
pq save "`tparquet'.parquet", replace
di c_2[1]
clear


pq use "`tparquet'.parquet", clear

di c_2[1]