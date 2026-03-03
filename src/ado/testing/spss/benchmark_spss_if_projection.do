set varabbrev off

// Benchmark: SPSS if-filter projection behavior and speed.
// This is a manual benchmark (not part of the default test harness).
// It compares:
//   1) full-column read + if()
//   2) subset-column read + if() where subset intentionally excludes filter var
// The subset path should still apply the filter correctly.

local spss_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\spss\data\sample_large.sav"
local reps = 20

capture confirm file "`spss_file'"
if _rc != 0 {
	di as error "Benchmark input file not found: `spss_file'"
	exit 601
}

// Inspect schema once.
pq use using "`spss_file'", clear format(spss)
count
local n_total = r(N)
ds
local all_vars `r(varlist)'
ds, has(type numeric)
local numeric_vars `r(varlist)'
local n_numeric : word count `numeric_vars'

if (`n_numeric' == 0) {
	di as error "No numeric variables found for filter benchmark."
	exit 9
}

// Pick a numeric filter variable with non-constant values.
local filter_var
local cutoff = .
foreach v of local numeric_vars {
	quietly summarize `v', meanonly
	if (r(min) < r(max)) {
		local filter_var `v'
		local cutoff = (r(min) + r(max)) / 2
		continue, break
	}
}

if ("`filter_var'" == "") {
	di as error "Could not find a non-constant numeric variable for filter benchmark."
	exit 9
}

local if_clause "`filter_var' > `cutoff'"

// Build a subset varlist that excludes filter_var when possible.
local subset_vars
foreach v of local all_vars {
	if ("`v'" != "`filter_var'") {
		local subset_vars `subset_vars' `v'
		local n_subset : word count `subset_vars'
		if (`n_subset' >= 3) {
			continue, break
		}
	}
}
if ("`subset_vars'" == "") {
	local subset_vars "`filter_var'"
}

// Baseline filtered row count.
pq use using "`spss_file'", clear format(spss) if(`if_clause')
count
local expected_rows = r(N)

if (`expected_rows' <= 0 | `expected_rows' >= `n_total') {
	di as text "note: filter selectivity is extreme (`expected_rows' of `n_total')."
}

di as text "Benchmark: SPSS if-filter projection"
di as text "File: `spss_file'"
di as text "Rows total: `n_total', filtered rows: `expected_rows'"
di as text "Filter: `if_clause'"
di as text "Subset vars (excluding filter var when possible): `subset_vars'"
di as text "Repetitions: `reps'"

timer clear

forvalues r = 1/`reps' {
	// Full read + if
	clear
	timer on 1
	pq use using "`spss_file'", clear format(spss) if(`if_clause')
	timer off 1
	assert _N == `expected_rows'

	// Subset read + if (filter var intentionally omitted from output subset)
	clear
	timer on 2
	pq use `subset_vars' using "`spss_file'", clear format(spss) if(`if_clause')
	timer off 2
	assert _N == `expected_rows'

	foreach v of local subset_vars {
		confirm variable `v'
	}
	if ("`subset_vars'" != "`filter_var'") {
		capture confirm variable `filter_var'
		assert _rc != 0
	}
}

timer list 1
local t_full = r(t1)
timer list 2
local t_subset = r(t2)
local ratio = `t_subset' / `t_full'
local speedup = `t_full' / `t_subset'

di as result _newline "Total elapsed seconds across repetitions:"
di as text "1: pq use (SPSS full) + if()"
timer list 1
di as text "2: pq use (SPSS subset) + if()"
timer list 2
di as result _newline "subset/full ratio: " %9.3f `ratio'
di as result "speedup (full/subset): " %9.3f `speedup' "x"

di as result _newline "Benchmark complete."
