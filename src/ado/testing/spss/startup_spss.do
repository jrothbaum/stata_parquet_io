set more off
set varabbrev off

local spss_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\spss\data\sample_large.sav"
local reps = 10
local subset_vars "mychar mynum mylabl"

capture confirm file "`spss_file'"
if _rc != 0 {
	di as error "Benchmark input file not found: `spss_file'"
	exit 601
}

import spss using "`spss_file'", clear
count
local n_rows = r(N)

di as text "Startup benchmark: SPSS"
di as text "File: `spss_file'"
di as text "Rows: `n_rows', Repetitions after first call: `reps'"

timer clear

// First full read
clear
timer on 1
pq use_spss using "`spss_file'", clear
timer off 1
assert _N == `n_rows'

// Repeated full reads
forvalues r = 1/`reps' {
	clear
	timer on 2
	pq use_spss using "`spss_file'", clear
	timer off 2
	assert _N == `n_rows'
}

// First subset read
clear
timer on 3
pq use_spss `subset_vars' using "`spss_file'", clear
timer off 3
assert _N == `n_rows'

// Repeated subset reads
forvalues r = 1/`reps' {
	clear
	timer on 4
	pq use_spss `subset_vars' using "`spss_file'", clear
	timer off 4
	assert _N == `n_rows'
}

di as result _newline "Total elapsed seconds:"
di as text "1: pq use_spss full (first call)"
timer list 1
di as text "2: pq use_spss full (avg repeated)"
timer list 2
di as text "3: pq use_spss subset (first call)"
timer list 3
di as text "4: pq use_spss subset (avg repeated)"
timer list 4

di as result _newline "Done."
