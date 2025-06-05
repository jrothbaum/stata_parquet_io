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
	
	
	
	
	tempfile path_save_root
	
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
		
		timer list
		local save_stata = r(t1)
		local save_parquet = r(t2)
		local use_stata = r(t3)
		local use_parquet = r(t4)
		local save_ratio = r(t2)/r(t1)
		local use_ratio = r(t4)/r(t3)
		noisily di "(" %15.0fc `n_rows' ", " %15.0fc `n_cols' ")"
		noisily di "	1: Stata:	save:	" %9.2f `save_stata'
		noisily di "	2: Parquet:	save:	" %9.2f `save_parquet' "	" %9.2f `save_ratio'
		noisily di "	3: Stata:	use:	" %9.2f `use_stata'
		noisily di "	4: Parquet:	use:	" %9.2f `use_parquet'  "	" %9.2f `use_ratio'
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

benchmark_parquet_io_data, 	n_cols(5000)	///
				n_rows(10000)

clear
