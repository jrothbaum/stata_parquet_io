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

local sizes "100 1000 10000 100000 1000000"
local reps = 3

tempfile base native_tmp
local native_csv : subinstr local native_tmp ".tmp" "_native.csv", all
if ("`native_csv'" == "`native_tmp'") local native_csv "`native_tmp'_native.csv"

capture erase "`native_csv'"

di as text "Benchmark: pq use_csv startup vs native import delimited"
di as text "Each size reports:"
di as text "  pq_first = first pq use_csv call (includes any one-time overhead)"
di as text "  pq_avg   = average of next `reps' pq use_csv calls"
di as text "  native_avg = average of `reps' import delimited calls"
di as text ""
di as text "n_rows | pq_first(s) | pq_avg(s) | native_avg(s)"
di as text "-----------------------------------------------"

set seed 20260304

foreach n of local sizes {
	build_bench_data, n_rows(`n')
	save "`base'", replace

	use "`base'", clear
	export delimited using "`native_csv'", replace

	timer clear

	// First pq read call for this size
	clear
	timer on 1
	pq use_csv using "`native_csv'", clear
	timer off 1
	assert _N == `n'
	assert id[1] == 1
	assert id[`n'] == `n'

	// Repeated pq reads (steady-state)
	forvalues r = 1/`reps' {
		clear
		timer on 2
		pq use_csv using "`native_csv'", clear
		timer off 2
		assert _N == `n'
	}

	// Repeated native reads
	forvalues r = 1/`reps' {
		clear
		timer on 3
		import delimited using "`native_csv'", varnames(1) clear
		timer off 3
		assert _N == `n'
	}

	timer list 1
	local pq_first = r(t1)
	timer list 2
	local pq_avg = r(t2) / `reps'
	timer list 3
	local native_avg = r(t3) / `reps'

	di as result ///
		"`=strtrim(string(`n',"%12.0gc"))' | " ///
		%9.4f `pq_first' " | " ///
		%8.4f `pq_avg' " | " ///
		%10.4f `native_avg'
}

capture erase "`native_csv'"

di as result _newline "Done."
