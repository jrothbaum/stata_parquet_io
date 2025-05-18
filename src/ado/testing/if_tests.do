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
	
	local n_if = `n_if' + 1
	local if_set`n_if' (c_3 > 0 & !missing(c_3)) & c_4 < 0
	
	local n_if = `n_if' + 1
	local if_set`n_if' (c_3 > 0 & !missing(c_3)) | c_4 < 0
	
	local n_if = `n_if' + 1
	local if_set`n_if' (c_3 > 0 & !missing(c_3)) | c_4 < 0 & !missing(c_5)
	
	local n_if = `n_if' + 1
	local if_set`n_if' (inrange(c_3, 0.5,1))
	
	local n_if = `n_if' + 1
	local if_set`n_if' inlist(c_1, 100,101, 500)
	
	local n_if = `n_if' + 1
	local if_set`n_if' c_2 == "A"
	
	local n_if = `n_if' + 1
	local if_set`n_if' c_2 == "B" & !missing(c_4) & (c_5 > 100 & !missing(c_5))
	
	
	local n_if = `n_if' + 1
	local if_set`n_if' c_2 == "B" & !missing(c_4) & c_5 > 100
	
	
	forvalues i = 1/`n_if' {
		pq use "`path_save_root'.parquet", clear 
		count
		di `"keep if `if_set`i''"'
		keep if `if_set`i''
		save "`path_save_root'.dta", replace
		pq use "`path_save_root'.parquet", clear if(`if_set`i'')
		
		
		unab all_vars: *
		rename * *_pq
		quietly merge 1:1 _n using "`path_save_root'.dta", nogen

		
		di `"N for `if_set`i'':	"' _N
		di `"Disagreements in for `if_set`i'':"'
		compare_files `all_vars'
		di _newline(2)
	}
	
	
	
	
	
	
	
	
	
	
	//	capture erase `path_save_root'.parquet
	//	capture erase `path_save_root'.dta
	
end

capture program drop compare_files
program compare_files
	syntax varlist
	
	
	
	foreach vari in `varlist' {
		quietly count if (`vari' != `vari'_pq) | (missing(`vari'_pq) & !missing(`vari'))
		local n_disagree = r(N)
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