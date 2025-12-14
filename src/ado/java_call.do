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

local tparquet C:\Users\jonro\Downloads\test_java.parquet

/*
create_data, n_rows(10000000) n_cols(10) 
compress
pq save "`tparquet'.parquet", replace
*/

//	Ignore the initial jar file load time
//	quietly {
{
	timer on 1
	pq use_java "`tparquet'.parquet", clear in(1/2)
	timer off 1
}
sum

clear
timer clear
count

count
timer on 1
pq use_java "`tparquet'.parquet", clear
timer off 1
count
sum


timer on 2
pq use "`tparquet'.parquet", clear
timer off 2
sum


timer list