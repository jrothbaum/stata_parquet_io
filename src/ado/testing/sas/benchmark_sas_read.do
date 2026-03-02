set more off
set varabbrev off

// Read benchmark for SAS:
// compares pq use_sas vs native import sas (full-read and subset-varlist).

local sas_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat\crates\polars_readstat_rs\tests\sas\data\too_big\hhpub25.sas7bdat"
local reps = 5

capture confirm file "`sas_file'"
if _rc != 0 {
	di as error "Benchmark input file not found: `sas_file'"
	exit 601
}

// Determine row count and construct a small subset varlist.
pq use_sas "`sas_file'", clear
count
local n_rows = r(N)
ds
local all_vars `r(varlist)'
local n_all : word count `all_vars'

if (`n_rows' <= 0) {
	di as error "Expected positive row count in benchmark file; got `n_rows'"
	exit 9
}
if (`n_all' == 0) {
	di as error "No variables found in benchmark file"
	exit 9
}

local subset_n = cond(`n_all' < 5, `n_all', 5)
local subset_vars
forvalues i = 1/`subset_n' {
	local vi : word `i' of `all_vars'
	local subset_vars `subset_vars' `vi'
}

local excluded_var
if (`n_all' > `subset_n') {
	local excluded_idx = `subset_n' + 1
	local excluded_var : word `excluded_idx' of `all_vars'
}

di as text "Benchmark: SAS read performance (pq vs native)"
di as text "File: `sas_file'"
di as text "Rows: `n_rows', Repetitions: `reps'"
di as text "Subset varlist: `subset_vars'"

timer clear

forvalues r = 1/`reps' {
	// pq read full
	clear
	timer on 1
	pq use_sas "`sas_file'", clear
	timer off 1
	assert _N == `n_rows'

	// pq read subset vars
	clear
	timer on 2
	pq use_sas `subset_vars' using "`sas_file'", clear
	timer off 2
	assert _N == `n_rows'
	foreach v in `subset_vars' {
		confirm variable `v'
	}
	if ("`excluded_var'" != "") {
		capture confirm variable `excluded_var'
		assert _rc != 0
	}

	// native read full
	clear
	timer on 3
	import sas using "`sas_file'", clear
	timer off 3
	assert _N == `n_rows'

	// native read subset vars
	clear
	timer on 4
	import sas `subset_vars' using "`sas_file'", clear
	timer off 4
	assert _N == `n_rows'
	foreach v in `subset_vars' {
		confirm variable `v'
	}
	if ("`excluded_var'" != "") {
		capture confirm variable `excluded_var'
		assert _rc != 0
	}
}

di as result _newline "Total elapsed seconds across repetitions:"
di as text "1: pq use_sas (read full)"
timer list 1
di as text "2: pq use_sas (read subset vars)"
timer list 2
di as text "3: import sas (read full)"
timer list 3
di as text "4: import sas (read subset vars)"
timer list 4

di as result _newline "Benchmark complete."
di as text "Interpretation: lower elapsed seconds is faster."
