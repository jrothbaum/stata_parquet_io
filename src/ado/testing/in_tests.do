//  If you don't care about all the options, here's the simplest version 
//      of how to work with parquet files
set varabbrev off

capture program drop in_test_parquet_io_data
program define in_test_parquet_io_data
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
	pq save "`path_save_root'.parquet", replace
	
	
	
	
	
	//	Several in sets
	local n_tenth = floor(`n_rows'/10)
	local n_in = 1
	local in_set`n_in' 1/`n_tenth'
	
	
	local n_in = `n_in' + 1
	local lower_n = (`n_tenth' + 1)
	local upper_n = (2*`n_tenth')
	local in_set`n_in' `lower_n'/`upper_n'
	
	
	forvalues i = 1/`n_in' {
		pq use "`path_save_root'.parquet", clear 
		count
		di "keep in `in_set`i''"
		keep in `in_set`i''
		save "`path_save_root'.dta", replace
		pq use "`path_save_root'.parquet", clear in(`in_set`i'')
		
		
		unab all_vars: *
		rename * *_pq
		quietly merge 1:1 _n using "`path_save_root'.dta", nogen

		
		di "N for `in_set`i'':	" _N
		di "Disagreements in for `in_set`i'':"
		compare_files `all_vars', do_assert
		di _newline(2)
	
	
		/*
		di "Test in on save"
		pq use "`path_save_root'.parquet", clear
		pq save "`path_save_root'_subset.parquet", replace in(`in_set`i'')
		
		pq use "`path_save_root'_subset.parquet", clear
		rename * *_pq
		quietly merge 1:1 _n using "`path_save_root'.dta", nogen

		di "N for `in_set`i'':	" _N
		di "Disagreements in for `in_set`i'':"
		compare_files `all_vars'

		di _newline(2)
		*/
	}
	
	
	local out_of_range_start = `n_rows' - `n_tenth'/2 + 1
	local out_of_range_end = `n_rows' + `n_tenth'/2
	di "Test of read beyond `n_rows'"
	pq use "`path_save_root'.parquet", clear in(`out_of_range_start'/`out_of_range_end')
	count
	sum
	di "Rows: " _N
	
	
	
	capture erase `path_save_root'.parquet
	//	capture erase `path_save_root'_subset.parquet
	capture erase `path_save_root'.dta
	
end

capture program drop compare_files
program compare_files
	syntax varlist, [do_assert]
	
	
	local var_count = 0
	foreach vari in `varlist' {
		local var_count = `var_count' + 1
		quietly count if (`vari' != `vari'_pq) | (missing(`vari'_pq) & !missing(`vari'))
		local n_disagree = r(N)
		
		if ("`do_assert'" != "") {
			if (`var_count' == 1)	di "Asserting no disagreements"
			assert `n_disagree' == 0
		}
		
		di as text "  " %-33s "`vari':" as result %8.0f r(N)
		
		
		if `n_disagree' {
			sum `vari' `vari'_pq
		}
	}

end


clear
set seed 1565225

in_test_parquet_io_data, 	n_cols(10)	///
								n_rows(10000)
clear
