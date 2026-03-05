set more off
set varabbrev off

// Benchmark: pq use  fast (cached) vs  normal (streaming) — parquet format
//
// fast   : describe collects full DataFrame into RAM and caches it;
//          the subsequent read skips the second disk scan entirely.
// normal : auto_fast_limit(0) ensures auto-fast never fires.

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
local reps  = 5

tempfile base pq_tmp
local pq_file "`pq_tmp'.parquet"
capture erase "`pq_file'"

di as text "Benchmark: pq use  fast (cached) vs  normal (streaming)  [parquet]"
di as text "Reps per cell: `reps'"
di as text ""
di as text "n_rows    | normal(s) | fast(s)  | speedup"
di as text "----------|-----------|----------|--------"

set seed 20260304

foreach n of local sizes {
	build_bench_data, n_rows(`n')
	save "`base'", replace

	use "`base'", clear
	pq save "`pq_file'", replace

	timer clear

	// ── Normal (streaming) ──────────────────────────────────────
	forvalues r = 1/`reps' {
		clear
		timer on 1
		pq use using "`pq_file'", clear auto_fast_limit(0)
		timer off 1
		assert _N == `n'
	}

	// ── Fast (collect+cache) ────────────────────────────────────
	forvalues r = 1/`reps' {
		clear
		timer on 2
		pq use using "`pq_file'", clear fast
		timer off 2
		assert _N == `n'
	}

	timer list 1
	local t_normal = r(t1) / `reps'
	timer list 2
	local t_fast   = r(t2) / `reps'
	local speedup  = `t_normal' / max(`t_fast', 1e-9)

	di as result ///
		%9.0gc `n' " | " ///
		%9.4f `t_normal' " | " ///
		%8.4f `t_fast' " | " ///
		%5.2f `speedup' "x"
}

capture erase "`pq_file'"

di as result _newline "Done."
