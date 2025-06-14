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
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_`ci' = rnormal()
		}
	}
end


di "Parallelization"
create_data, n_rows(100000) n_cols(10) 
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
sum
pq use c_* using "`tparquet'.parquet", clear
pq use c_1* c_4 using "`tparquet'.parquet", clear

sum


pq use "`tparquet'.parquet", clear sort(c_2 c_1)
forvalues i=1/10 {
	di c_1[`i']
	di c_2[`i']
}

pq use "`tparquet'.parquet", clear sort(-c_2 -c_1)
forvalues i=1/10 {
	di c_1[`i']
	di c_2[`i']
}



pq use * using "`tparquet'.parquet", clear compress
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

sum
pq use "`tparquet'_2018.parquet", clear
sum
describe
pq append "`tparquet'_2019.parquet", compress
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

pq use "`tparquet'_2018.parquet", clear
pq merge 1:1 _n using "`tparquet'_merge.parquet", compress



capture erase `tparquet'_2018.parquet
capture erase `tparquet'_2019.parquet

clear