set more off
set varabbrev off

capture program drop build_bench_data
program define build_bench_data
	version 16
	syntax, n_rows(integer)

	clear
	set obs `n_rows'

	// 10 variables total
	gen long   id   = _n
	gen byte   grp  = mod(_n, 20)
	gen double x1   = rnormal()
	gen double x2   = runiform() * 1000
	gen double x3   = (rnormal() * 10) + grp
	gen double x4   = sqrt(_n)
	gen int    i1   = floor(runiform() * 32740)
	gen int    i2   = floor(runiform() * 5000) - 2500
	gen str8   s1   = char(65 + mod(_n, 26)) + string(mod(_n, 100))
	gen str20  s2   = "row_" + string(_n) + "_g" + string(grp)
end

local n_rows = 100000
local reps = 3

set seed 20260301
build_bench_data, n_rows(`n_rows')

tempfile base pq_tmp native_tmp
save "`base'", replace

local pq_csv : subinstr local pq_tmp ".tmp" "_pq.csv", all
if ("`pq_csv'" == "`pq_tmp'") local pq_csv "`pq_tmp'_pq.csv"

local native_csv : subinstr local native_tmp ".tmp" "_native.csv", all
if ("`native_csv'" == "`native_tmp'") local native_csv "`native_tmp'_native.csv"

capture erase "`pq_csv'"
capture erase "`native_csv'"

di as text "Benchmark: CSV read/write"
di as text "Rows: `n_rows', Variables: 10, Repetitions: `reps'"
di as text "Subset read varlist: id grp x1 s1"

timer clear

forvalues r = 1/`reps' {
	// pq write
	use "`base'", clear
	timer on 1
	pq save_csv "`pq_csv'", replace
	timer off 1

	// pq read full
	clear
	timer on 2
	pq use_csv "`pq_csv'", clear
	timer off 2
	assert _N == `n_rows'
	assert id[1] == 1
	assert id[`n_rows'] == `n_rows'

	// pq read subset vars
	clear
	timer on 3
	pq use_csv id grp x1 s1 using "`pq_csv'", clear
	timer off 3
	assert _N == `n_rows'
	confirm variable id grp x1 s1
	capture confirm variable x2
	assert _rc != 0

	// native write
	use "`base'", clear
	timer on 4
	export delimited using "`native_csv'", replace
	timer off 4

	// native read full
	clear
	timer on 5
	import delimited using "`native_csv'", varnames(1) clear
	timer off 5
	assert _N == `n_rows'
	assert id[1] == 1
	assert id[`n_rows'] == `n_rows'

	// native read subset vars (import delimited has no column projection; full load then keep)
	clear
	timer on 6
	import delimited using "`native_csv'", varnames(1) clear
	keep id grp x1 s1
	timer off 6
	assert _N == `n_rows'
	confirm variable id grp x1 s1
	capture confirm variable x2
	assert _rc != 0
}

di as result _newline "Total elapsed seconds across repetitions:"
di as text "1: pq save_csv (write)"
timer list 1
di as text "2: pq use_csv (read full)"
timer list 2
di as text "3: pq use_csv (read subset vars)"
timer list 3
di as text "4: export delimited (write)"
timer list 4
di as text "5: import delimited (read full)"
timer list 5
di as text "6: import delimited (read subset vars)"
timer list 6

di as result _newline "Benchmark complete."
di as text "Interpretation: lower elapsed seconds is faster."

capture erase "`pq_csv'"
capture erase "`native_csv'"
