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
local tmp_root C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\test_data\tmp
capture mkdir "`tmp_root'"
local tparquet `tmp_root'\test_save

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

pq save "`tparquet'_if.parquet", replace if(c_1 <= 1000 | c_1 > 90000)

clear

//	Add new data to an existing partition
create_data, n_rows(100) n_cols(100) 
replace year = 1
pq save "`tparquet'_partitioned.parquet", replace partition_by(year month) nopartitionoverwrite

replace year = 0
pq save "`tparquet'_partitioned.parquet", replace partition_by(year month) nopartitionoverwrite




pq use "`tparquet'_gzip.parquet", clear
sum
pq use "`tparquet'_gzip_2.parquet", clear
sum
pq use "`tparquet'_partitioned.parquet", clear
sum
pq use "`tparquet'_if.parquet", clear
sum


//	Save that should fail
pq save "`tmp_root'\non_hive.parquet", partition_by(year) replace



recast double c_1
tostring c_10, replace force
describe
sum
pq save "`tmp_root'\compress.parquet", replace compress compress_string_to_numeric

pq use "`tmp_root'\compress.parquet", clear
describe
sum


clear
set obs 4
gen double state = cond(_n <= 2, 1, 2)
gen value = _n
local tpartition_float `tmp_root'\test_save_partition_float.parquet
pq save "`tpartition_float'", replace partition_by(state)
assert fileexists("`tpartition_float'\state=1.0\data_0.parquet")
assert fileexists("`tpartition_float'\state=2.0\data_0.parquet")

clear
set obs 4
gen int state = cond(_n <= 2, 1, 2)
gen value = _n
local tpartition_int `tmp_root'\test_save_partition_int.parquet
pq save "`tpartition_int'", replace partition_by(state)
assert fileexists("`tpartition_int'\state=1\data_0.parquet")
assert fileexists("`tpartition_int'\state=2\data_0.parquet")
