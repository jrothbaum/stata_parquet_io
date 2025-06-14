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



tempfile tparquet
create_data, n_rows(100) n_cols(10) 
pq save "`tparquet'.parquet", replace

sum
recast str100 c_2 
tostring c_4, replace
sum
forvalues i = 2/10 {
	rename c_`i' c_`=`i'+10'
}
pq save "`tparquet'_merge.parquet", replace

pq use "`tparquet'.parquet", clear
pq merge 1:1 c_1 using "`tparquet'_merge.parquet"

pq use "`tparquet'.parquet", clear
pq merge 1:1 _n using "`tparquet'_merge.parquet", compress compress_string_to_numeric



pq use "`tparquet'.parquet", clear
pq merge 1:1 _n using "`tparquet'_merge.parquet", compress random_n(50)



pq use "`tparquet'.parquet", clear
pq merge 1:1 _n using "`tparquet'_merge.parquet", compress in(1/90)



capture erase `tparquet'.parquet
capture erase `tparquet'_merge.parquet

clear