//  If you don't care about all the options, here's the simplest version 
//      of how to work with parquet files


capture program drop benchmark_parquet_io_data
program define benchmark_parquet_io_data
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
	
	local n_to_load = 5
	local subset_to_load
	forvalues i=1/`n_to_load' {
		local subset_to_load `subset_to_load' c_`i'
	}
	
	
	
	tempfile path_save_root
	local path_save_root C:\Users\jonro\Downloads\test_benchmark
	quietly {
		timer clear
		di "save stata"
		timer on 1
		save "`path_save_root'.dta", replace
		timer off 1
		
		di "save parquet"
		timer on 2
		
		di `"pq save "`path_save_root'.parquet", replace"'
		pq save "`path_save_root'.parquet", replace
		timer off 2
		
		di "use stata"
		timer on 3
		use "`path_save_root'.dta", clear
		timer off 3
		
		di "use parquet"
		timer on 4
		di `"pq use "`path_save_root'.parquet", clear"'
		pq use "`path_save_root'.parquet", clear
		timer off 4
		
		
		di "use stata"
		timer on 5
		use `subset_to_load' using "`path_save_root'.dta", clear
		timer off 5
		
		di "use parquet"
		timer on 6
		di `"pq use "`path_save_root'.parquet", clear"'
		pq use `subset_to_load' using "`path_save_root'.parquet", clear
		timer off 6
		
		timer list
		local save_stata = r(t1)
		local save_parquet = r(t2)
		local use_stata = r(t3)
		local use_parquet = r(t4)
		local use_stata_subset = r(t5)
		local use_parquet_subset = r(t6)
		local save_ratio = r(t2)/r(t1)
		local use_ratio = r(t4)/r(t3)
		local use_ratio_subset = r(t6)/r(t5)
		noisily di "(" %15.0fc `n_rows' ", " %15.0fc `n_cols' ")"
		noisily di "	1: Stata:	save:	" %9.2f `save_stata'
		noisily di "	2: Parquet:	save:	" %9.2f `save_parquet' "	" %9.2f `save_ratio'
		noisily di "	3: Stata:	use:	" %9.2f `use_stata'
		noisily di "	4: Parquet:	use:	" %9.2f `use_parquet'  "	" %9.2f `use_ratio'
		
		noisily di ""
		noisily di "	Loading only `n_to_load' variables of `n_cols'"
		noisily di "	5: Stata:	use:	" %9.2f `use_stata_subset'
		noisily di "	6: Parquet:	use:	" %9.2f `use_parquet_subset'  "	   " %9.2f `use_ratio_subset'
	}
	
	capture erase `path_save_root'.parquet
	capture erase `path_save_root'.dta
	
end


clear
set seed 1565225

benchmark_parquet_io_data, 	n_cols(10)	///
				n_rows(1000)
				

benchmark_parquet_io_data, 	n_cols(10)	///
				n_rows(10000)
				
benchmark_parquet_io_data, 	n_cols(10)	///
				n_rows(100000)
				
				
benchmark_parquet_io_data, 	n_cols(10)	///
				n_rows(1000000)
				
				
benchmark_parquet_io_data, 	n_cols(10)	///
				n_rows(10000000)

benchmark_parquet_io_data, 	n_cols(100)	///
				n_rows(1000000)

benchmark_parquet_io_data, 	n_cols(1000)	///
				n_rows(100000)

clear