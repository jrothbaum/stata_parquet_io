set more off
set varabbrev off

// Benchmark: pq use_spss  fast (cached) vs  normal (streaming)
//
// fast   : describe collects full DataFrame into RAM and caches it;
//          the subsequent read skips the second disk scan entirely.
// normal : auto_fast_limit(0) ensures auto-fast never fires.

local spss_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\spss\data\sample_large.sav"
local reps = 20

capture confirm file "`spss_file'"
if _rc != 0 {
	di as error "Benchmark input file not found: `spss_file'"
	exit 601
}

// Determine row count once
pq use_spss using "`spss_file'", clear auto_fast_limit(0)
count
local n_rows = r(N)

di as text "Benchmark: pq use_spss  fast (cached) vs  normal (streaming)"
di as text "File: `spss_file'"
di as text "Rows: `n_rows', Reps per cell: `reps'"
di as text ""
di as text "mode   | avg(s)   | speedup"
di as text "-------|----------|--------"

timer clear

// ── Normal (streaming) ──────────────────────────────────────────
forvalues r = 1/`reps' {
	clear
	timer on 1
	pq use_spss using "`spss_file'", clear auto_fast_limit(0)
	timer off 1
	assert _N == `n_rows'
}

// ── Fast (collect+cache) ─────────────────────────────────────────
forvalues r = 1/`reps' {
	clear
	timer on 2
	pq use_spss using "`spss_file'", clear fast
	timer off 2
	assert _N == `n_rows'
}

timer list 1
local t_normal = r(t1) / `reps'
timer list 2
local t_fast   = r(t2) / `reps'
local speedup  = `t_normal' / max(`t_fast', 1e-9)

di as result "normal | " %8.4f `t_normal' " | (baseline)"
di as result "fast   | " %8.4f `t_fast'   " | " %5.2f `speedup' "x"

di as result _newline "Done."
