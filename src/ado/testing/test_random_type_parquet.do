//	Test against various packages test data



capture program drop test_file
program define test_file
	version 16
	syntax		, 	path(string)

	
	tempfile path_save_root
	
	
	pq use "`path'.parquet", clear
	save "`path_save_root'.dta", replace
	pq save "`path_save_root'.parquet", replace
	
	
	pq use "`path_save_root'.parquet", clear
	
	unab all_vars: *
	rename * *_pq
	quietly merge 1:1 _n using "`path_save_root'.dta", nogen
	
	di "N for `in_set`i'':	" _N
	di "Disagreements in for `in_set`i'':"
	compare_files `all_vars', do_assert
	di _newline(2)
	
	capture erase `path_save_root'.parquet
	
	di _newline(2)
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






test_file, path(C:\Users\jonro\Downloads\random_types)


pq use "C:\Users\jonro\Downloads\random_types_partitioned.parquet\**.parquet", clear