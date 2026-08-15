set more off
set varabbrev off

global PARQIT_PLUGIN_PATH "C:\Users\jonro\ado\plus\p\parqit_win64.plugin"

// Basic load/save benchmark: pq (stata_parquet_io) vs parqit
//
// pq     : I/O bridge. `pq save` writes memory->parquet; `pq use` reads
//          parquet->memory directly.
// parqit : out-of-core manipulation layer. `parqit use using F` (no `clear`)
//          opens a lazy view with no rows loaded; passing `clear` skips the
//          view and materialises straight into memory instead, which is the
//          apples-to-apples counterpart to `pq use`.
//          `parqit save F, replace data` writes memory->parquet, matching
//          `pq save`.

capture program drop build_bench_data
program define build_bench_data
	version 16
	syntax, n_rows(integer)

	clear
	set obs `n_rows'
	gen long   c1 = _n
	gen str8   c2 = char(65 + mod(_n, 26)) + string(mod(_n, 100))
	forvalues i = 3/10 {
		gen double c`i' = rnormal()
	}
end

local sizes "1000 10000 100000 1000000 10000000"
local reps  = 3

tempfile base
tempfile pq_tmp parqit_tmp
local pq_file     "`pq_tmp'_pq.parquet"
local parqit_file "`parqit_tmp'_parqit.parquet"

di as text "Benchmark: pq  vs  parqit  — basic load/save"
di as text "10 vars (1 long, 1 str8, 8 double). Reps per cell: `reps' (reporting min)."
di as text ""
di as text "rows        | pq save | parqit save | pq use | parqit use"
di as text "------------|---------|-------------|--------|------------"

set seed 20260815

foreach n of local sizes {
	build_bench_data, n_rows(`n')
	save "`base'", replace

	local t_save_pq     = 1e9
	local t_save_parqit = 1e9
	local t_use_pq      = 1e9
	local t_use_parqit  = 1e9

	forvalues r = 1/`reps' {
		use "`base'", clear
		capture erase "`pq_file'"
		timer clear 1
		timer on 1
		quietly pq save "`pq_file'", replace
		timer off 1
		quietly timer list 1
		local t_save_pq = min(`t_save_pq', r(t1))

		use "`base'", clear
		capture parqit close _all
		capture erase "`parqit_file'"
		timer clear 2
		timer on 2
		quietly parqit save "`parqit_file'", replace data
		timer off 2
		quietly timer list 2
		local t_save_parqit = min(`t_save_parqit', r(t2))

		clear
		timer clear 3
		timer on 3
		quietly pq use using "`pq_file'", clear
		timer off 3
		quietly timer list 3
		assert _N == `n'
		local t_use_pq = min(`t_use_pq', r(t3))

		clear
		capture parqit close _all
		timer clear 4
		timer on 4
		quietly parqit use using "`parqit_file'", clear
		timer off 4
		quietly timer list 4
		assert _N == `n'
		local t_use_parqit = min(`t_use_parqit', r(t4))
	}

	di as result ///
		%11.0gc `n' " | " ///
		%7.4f `t_save_pq' " | " ///
		%11.4f `t_save_parqit' " | " ///
		%6.4f `t_use_pq' " | " ///
		%10.4f `t_use_parqit'
}

capture parqit close _all
capture erase "`pq_file'"
capture erase "`parqit_file'"

di as result _newline "Done."
