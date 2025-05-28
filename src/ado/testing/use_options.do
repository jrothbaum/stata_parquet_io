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
		quietly gen c_`cols_created' = _n
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = char(65 + floor(runiform()*5))
	}
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_`ci' = rnormal()
		}
	}
end


di "Parallelization"
create_data, n_rows(100000) n_cols(100) 
tempfile tparquet

pq save "`tparquet'.parquet", replace

clear
timer clear
timer on 1
pq use "`tparquet'.parquet", clear parallelize(columns)
timer off 1

clear
timer on 2
pq use "`tparquet'.parquet", clear parallelize(rows)
timer off 2

di "1:	Columns"
di "2:	Rows"
timer list

pq use * using "`tparquet'.parquet", clear 
pq use c_* using "`tparquet'.parquet", clear
pq use c_1* using "`tparquet'.parquet", clear



capture erase `tparquet'.parquet






di "Asterisk as variable name"
create_data, n_rows(100) n_cols(10) 
pq save "`tparquet'_2018.parquet", replace
pq save "`tparquet'_2019.parquet", replace

clear

pq describe "`tparquet'_*.parquet", asterisk_to_variable(year)
return list
pq use "`tparquet'_*.parquet", clear asterisk_to_variable(year)

sum

capture erase `tparquet'_2018.parquet
capture erase `tparquet'_2019.parquet

clear