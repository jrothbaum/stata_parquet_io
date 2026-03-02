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

create_data, n_rows(100000) n_cols(100)
tempfile tcsv

pq save_csv "`tcsv'.csv", replace
pq use_csv "`tcsv'.csv", clear
assert _N == 100000

// Save with if() should work for CSV
pq save_csv "`tcsv'_if.csv", replace if(c_1 <= 1000 | c_1 > 90000)
pq use_csv "`tcsv'_if.csv", clear
assert _N == 11000

// compress flags are format-agnostic and should work
pq use_csv "`tcsv'.csv", clear
recast double c_1
tostring c_10, replace force
pq save_csv "`tcsv'_compress.csv", replace compress compress_string_to_numeric
pq use_csv "`tcsv'_compress.csv", clear
assert _N == 100000

// parquet-only options should fail for CSV
capture pq save_csv "`tcsv'_gzip.csv", replace compression(gzip)
assert _rc != 0

capture pq save_csv "`tcsv'_gzip2.csv", replace compression(gzip) compression_level(2)
assert _rc != 0

capture pq save_csv "`tcsv'_partition.csv", replace partition_by(year)
assert _rc != 0

capture pq save_csv "`tcsv'_partition.csv", replace partition_by(year month) nopartitionoverwrite
assert _rc != 0

capture pq save_csv "`tcsv'_stream.csv", replace chunk(1000)
assert _rc != 0

capture pq save_csv "`tcsv'_stream.csv", replace chunk(1000) stream
assert _rc != 0

di as result "All CSV save_options mirror tests PASSED"
