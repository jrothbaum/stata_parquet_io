//  If you don't care about all the options, here's the simplest version 
//      of how to work with parquet files


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
			local n_not_missing = (`n_rows'*(1-0.1*runiform()))
			quietly gen c_`ci' = rnormal() if _n < `n_not_missing'
		}
	}
	
	
	tempfile path_save_root
	local path_save_root C:\Users\jonro\Downloads\test_benchmark
	pq save "`path_save_root'.parquet", replace
	
	
	
	
	//	Several if statements
	local n_if = 1
	local if_set`n_if' (c_3 > 0 & !missing(c_3))
	local assert`n_if' 1
	
	local n_if = `n_if' + 1
	local if_set`n_if' (c_3 > 0 & !missing(c_3)) & c_4 < 0
	local assert`n_if' 1
	
	local n_if = `n_if' + 1
	local if_set`n_if' (c_3 > 0 & !missing(c_3)) | c_4 < 0
	local assert`n_if' 1
	
	local n_if = `n_if' + 1
	local if_set`n_if' (c_3 > 0 & !missing(c_3)) | c_4 < 0 & !missing(c_5)
	local assert`n_if' 1
	
	local n_if = `n_if' + 1
	local if_set`n_if' (inrange(c_3, 0.5,1))
	local assert`n_if' 1
	
	local n_if = `n_if' + 1
	local if_set`n_if' inlist(c_1, 100,101, 500)
	local assert`n_if' 1
	
	local n_if = `n_if' + 1
	local if_set`n_if' c_2 == "A"
	local assert`n_if' 1
	
	local n_if = `n_if' + 1
	local if_set`n_if' c_2 == "B" & !missing(c_4) & (c_5 > 100 & !missing(c_5))
	local assert`n_if' 0
	
	
	local n_if = `n_if' + 1
	local if_set`n_if' c_2 == "B" & !missing(c_4) & c_5 > 100
	local assert`n_if' 0
	
	
	forvalues i = 1/`n_if' {
		di "`i'"
		pq use "`path_save_root'.parquet", clear 
		count
		di `"keep if `if_set`i''"'
		keep if `if_set`i''
		save "`path_save_root'.dta", replace
		
		di "Test if on load"
		pq use "`path_save_root'.parquet", clear if(`if_set`i'')
		
		
		unab all_vars: *
		rename * *_pq
		quietly merge 1:1 _n using "`path_save_root'.dta", nogen

		local do_assert
		if (0`assert`i'') local do_assert do_assert
		
		di `"N for `if_set`i'':	"' _N
		di `"Disagreements in for `if_set`i'':"'
		compare_files `all_vars', `do_assert'

		di _newline(2)
		
		
		
		
		
		di "Test if on save"
		pq use "`path_save_root'.parquet", clear
		pq save "`path_save_root'_subset.parquet", replace if(`if_set`i'')
		
		pq use "`path_save_root'_subset.parquet", clear
		rename * *_pq
		quietly merge 1:1 _n using "`path_save_root'.dta", nogen

		di `"N for `if_set`i'':	"' _N
		di `"Disagreements in for `if_set`i'':"'
		compare_files `all_vars'

		di _newline(2)
		
	}
	
	
	
	
	di "Test saving a subset of variables"
	pq use "`path_save_root'.parquet", clear
	pq save c_1 c_2 c_3 using "`path_save_root'_subset.parquet", replace
	pq use "`path_save_root'_subset.parquet", clear
	sum
	
	
	pq use "`path_save_root'.parquet", clear
	pq save c_* using "`path_save_root'_subset.parquet", replace
	pq use "`path_save_root'_subset.parquet", clear
	sum
	
	pq use "`path_save_root'.parquet", clear
	pq save c_1-c_3 using "`path_save_root'_subset.parquet", replace
	pq use "`path_save_root'_subset.parquet", clear
	sum
	
	
	
	
	capture erase `path_save_root'.parquet
	capture erase `path_save_root'_subset.parquet
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