//	Test against various packages test data



capture program drop test_file
program define test_file
	version 16
	syntax		, 	path(string)

	
	tempfile path_save_root
	
	
	use "`path'.dta", clear
	
	pq save "`path_save_root'.parquet", replace
	
	
	pq use "`path_save_root'.parquet", clear

	
	unab all_vars: *
	rename * *_pq
	quietly merge 1:1 _n using "`path'.dta", nogen
	
	di "Disagreements in `path':"
	foreach vari in `all_vars' {
		quietly count if `vari' != `vari'_pq
		local n_disagree = r(N)
		di as text "  " %-33s "`vari':" as result %8.0f r(N)
		
		if `n_disagree' {
			sum `vari' `vari'_pq
		}
		
		//	assert `n_disagree' == 0
	}
	
	capture erase `path_save_root'.parquet
	
	di _newline(2)
end



local test_root C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io
test_file, path(`test_root'\test_data\pyreadstat\sample)

local path_haven `test_root'\test_data\haven
local files_haven notes tagged-na-double tagged-na-int types
foreach fi in `files_haven' {
	test_file, path(`path_haven'/`fi')
}




local path_pandas `test_root'\test_data\pandas
local files_pandas stata9_117 stata12_118 stata-compat-118 stata-compat-be-118
foreach fi in `files_pandas' {
	test_file, path(`path_pandas'/`fi')
}


//	https://opportunityinsights.org/data/
//	https://www.nber.org/research/data
local path_econ `test_root'\test_data\econ_data
local files_econ Table_4_cz_by_cohort_estimates Table_5_national_estimates_by_cohort_primary_outcomes county_population 20zpallagi Fin_Patent_Data_for_Posting.20220403 tm_assignment LLM_match_formulas_all tm_assignee
foreach fi in `files_econ' {
	test_file, path(`path_econ'/`fi')
}


