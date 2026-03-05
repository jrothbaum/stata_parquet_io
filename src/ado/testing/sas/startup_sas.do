set more off
set varabbrev off

local sas_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat\crates\polars_readstat_rs\tests\sas\data\too_big\hhpub25.sas7bdat"
local reps = 5

capture confirm file "`sas_file'"
if _rc != 0 {
	di as error "Benchmark input file not found: `sas_file'"
	exit 601
}

pq use_sas using "`sas_file'", clear
count
local n_rows = r(N)
ds
local all_vars `r(varlist)'

local subset_n = cond(`: word count `all_vars'' < 5, `: word count `all_vars'', 5)
local subset_vars
forvalues i = 1/`subset_n' {
	local vi : word `i' of `all_vars'
	local subset_vars `subset_vars' `vi'
}

di as text "Startup benchmark: SAS"
di as text "File: `sas_file'"
di as text "Rows: `n_rows', Repetitions after first call: `reps'"
di as text "Subset vars: `subset_vars'"

timer clear

// First full read
clear
timer on 1
pq use_sas using "`sas_file'", clear
timer off 1
assert _N == `n_rows'

// Repeated full reads
forvalues r = 1/`reps' {
	clear
	timer on 2
	pq use_sas using "`sas_file'", clear
	timer off 2
	assert _N == `n_rows'
}

// First subset read
clear
timer on 3
pq use_sas `subset_vars' using "`sas_file'", clear
timer off 3
assert _N == `n_rows'

// Repeated subset reads
forvalues r = 1/`reps' {
	clear
	timer on 4
	pq use_sas `subset_vars' using "`sas_file'", clear
	timer off 4
	assert _N == `n_rows'
}

di as result _newline "Total elapsed seconds:"
di as text "1: pq use_sas full (first call)"
timer list 1
di as text "2: pq use_sas full (avg repeated)"
timer list 2
di as text "3: pq use_sas subset (first call)"
timer list 3
di as text "4: pq use_sas subset (avg repeated)"
timer list 4

di as result _newline "Done."
