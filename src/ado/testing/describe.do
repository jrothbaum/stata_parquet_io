//	Test against various packages test data
set varabbrev off

capture program drop test_file
program define test_file
	version 16
	syntax		, 	path(string)

	
	tempfile path_save_root
	
	
	use "`path'.dta", clear
	
	pq save "`path_save_root'.parquet", replace
	
	unab all_vars: *
	
	local n_columns_stata = 0
	local n_rows_stata = _N
	foreach vari in `all_vars' {
		local n_columns_stata = `n_columns_stata' + 1
		
		local var_`n_columns_stata' `vari'
		local type_`n_columns_stata': type `vari'
		local format_`n_columns_stata': format `vari'
		local str_length_`n_columns_stata' 0
		
		if ((substr("`type_`n_columns_stata''",1,3) == "str") & ("`type_`n_columns_stata''" != "strl")) {
			local str_length_`n_columns_stata' = substr("`type_`n_columns_stata''",4,.)
			local type_`n_columns_stata' String
		}
		else {
			local type_`n_columns_stata' = strproper("`type_`n_columns_stata''")
			//	di "type_`n_columns_stata': `type_`n_columns_stata''"
		}
		
		//	di "var_`n_columns_stata': 		`var_`n_columns_stata''"
		//	di "type_`n_columns_stata': 		`type_`n_columns_stata''"
		//	di "format_`n_columns_stata': 		`format_`n_columns_stata''"
		//	di "str_length_`n_columns_stata': 	`str_length_`n_columns_stata''"
		
	}
	quietly {
		//	Just make sure they run
		pq describe using "`path_save_root'.parquet"
		pq describe "`path_save_root'.parquet", detailed
		pq describe "`path_save_root'.parquet", detailed quietly
	}
	
	local n_columns = r(n_columns)
	local n_rows = r(n_rows)
	// Define column widths
	local col1width 25
	local col2width 25
	local col3width 25

	// Calculate positions
	local pos_stata = int(`col1width'+4)
	local pos_pq = int(`col1width'/2+`col2width'/2 - 2)
	local pos_name1 = `col1width'
	local pos_name2 = `col1width'+`col2width'
	local total_width = `col1width'+`col2width'+`col3width'

	// Display header using format specifiers instead of _col
	di as text "{hline `total_width'}"
	di as text %`pos_stata's "Stata" %`pos_pq's "pq"
	di as text "{hline `total_width'}"

	forvalues i = 1/`n_columns' {
		local var_pq = r(name_`i')
		local type_pq = strproper(r(type_`i'))
		local str_length_pq = r(string_length_`i')
		
		// Define column widths
		

		// Display variable details using format specifiers
		di as text "Name:" %`col1width's "`var_`i''" %`col2width's "`var_pq'"
		di as text "Type:" %`col1width's "`type_`i'' (`format_`i'')" %`col2width's "`type_pq'"
		if ("`type_pq'" == "String")	di as text "Str#:" %`col1width's "`str_length_`i''" %`col2width's "`str_length_pq'"
		
		di as text "{hline `total_width'}"

/*
		di "			Stata			pq"
		di "Name:		`name_`i''		`var_pq'"
		di "Type:		`type_`i'' (`format_`i'')		`type_pq'"
		di "str_length:	`str_length_`i''		`str_length_pq'"
	*/
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
local files_econ Table_4_cz_by_cohort_estimates Table_5_national_estimates_by_cohort_primary_outcomes county_population 20zpallagi Fin_Patent_Data_for_Posting.20220403 tm_assignment LLM_match_formulas_all //	tm_assignee
foreach fi in `files_econ' {
	capture confirm file "`path_econ'/`fi'.dta"
	if (_rc == 0) {
		test_file, path(`path_econ'/`fi')
	}
	else {
		di as text "Skipping missing file: `path_econ'/`fi'.dta"
	}
}



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
local files_econ Table_4_cz_by_cohort_estimates Table_5_national_estimates_by_cohort_primary_outcomes county_population 20zpallagi Fin_Patent_Data_for_Posting.20220403 tm_assignment LLM_match_formulas_all //	tm_assignee
foreach fi in `files_econ' {
	capture confirm file "`path_econ'/`fi'.dta"
	if (_rc == 0) {
		test_file, path(`path_econ'/`fi')
	}
	else {
		di as text "Skipping missing file: `path_econ'/`fi'.dta"
	}
}


