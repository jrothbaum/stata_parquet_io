set more off
set varabbrev off

// Read benchmark for SPSS:
// compares pq use_spss vs Stata import spss on the same .sav file.
//
// Note: Stata has import spss but no native SPSS writer, so this benchmark is
// read-only (full-file and subset-varlist reads).

local spss_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\spss\data\sample_large.sav"
local reps = 20
local subset_vars "mychar mynum mylabl"

capture confirm file "`spss_file'"
if _rc != 0 {
	di as error "Benchmark input file not found: `spss_file'"
	exit 601
}

// Determine expected row count once
import spss using "`spss_file'", clear
count
local n_rows = r(N)

di as text "Benchmark: SPSS read performance"
di as text "File: `spss_file'"
di as text "Rows: `n_rows', Repetitions: `reps'"
di as text "Subset varlist: `subset_vars'"

timer clear

forvalues r = 1/`reps' {
	// pq read full
	clear
	timer on 1
	pq use using "`spss_file'", clear format(spss)
	timer off 1
	assert _N == `n_rows'

	// pq read subset vars
	clear
	timer on 2
	pq use `subset_vars' using "`spss_file'", clear format(spss)
	timer off 2
	assert _N == `n_rows'
	foreach v in `subset_vars' {
		confirm variable `v'
	}
	capture confirm variable dtime
	assert _rc != 0

	// native read full
	clear
	timer on 3
	import spss using "`spss_file'", clear
	timer off 3
	assert _N == `n_rows'

	// native read subset vars (import spss supports varlist)
	clear
	timer on 4
	import spss `subset_vars' using "`spss_file'", clear
	timer off 4
	assert _N == `n_rows'
	foreach v in `subset_vars' {
		confirm variable `v'
	}
	capture confirm variable dtime
	assert _rc != 0
}

di as result _newline "Total elapsed seconds across repetitions:"
di as text "1: pq use_spss (read full)"
timer list 1
di as text "2: pq use_spss (read subset vars)"
timer list 2
di as text "3: import spss (read full)"
timer list 3
di as text "4: import spss (read subset vars)"
timer list 4

di as result _newline "Benchmark complete."
di as text "Interpretation: lower elapsed seconds is faster."
