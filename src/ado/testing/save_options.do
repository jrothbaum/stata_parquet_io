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
		quietly gen int year = floor(runiform()*5) + 2017
	}
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen int month = floor(runiform()*12) + 1
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
local tparquet C:\Users\jonro\Downloads\test_save

pq save "`tparquet'.parquet", replace
pq use "`tparquet'.parquet", clear


//	Save with different compression
pq save "`tparquet'_gzip.parquet", replace compression(gzip)
pq save "`tparquet'_gzip_2.parquet", replace compression(gzip) compression_level(2)

//	Save partition
pq save "`tparquet'_partitioned.parquet", replace partition_by(year)

//	Replace a partition with a normal file
pq save "`tparquet'_partitioned.parquet", replace

//	Save another partition
pq save "`tparquet'_partitioned.parquet", replace partition_by(year month)

clear

//	Add new data to an existing partition
create_data, n_rows(100) n_cols(100) 
replace year = 2030
pq save "`tparquet'_partitioned.parquet", replace partition_by(year month) nopartitionoverwrite


pq use "`tparquet'_gzip.parquet", clear
sum
pq use "`tparquet'_gzip_2.parquet", clear
sum
pq use "`tparquet'_partitioned.parquet", clear
sum
