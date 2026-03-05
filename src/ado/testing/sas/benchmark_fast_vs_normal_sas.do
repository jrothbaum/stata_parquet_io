set more off
set varabbrev off

// Benchmark: pq use_sas  fast (cached) vs  normal (streaming)
//
// fast   : describe collects full DataFrame into RAM and caches it;
//          the subsequent read skips the second disk scan entirely.
// normal : auto_fast_limit(0) ensures auto-fast never fires.

local sas_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat\crates\polars_readstat_rs\tests\sas\data\too_big\hhpub25.sas7bdat"
local reps = 5

capture confirm file "`sas_file'"
if _rc != 0 {
	di as error "Benchmark input file not found: `sas_file'"
	exit 601
}

// Determine row count once (normal mode to avoid warming cache)
pq use_sas using "`sas_file'", clear auto_fast_limit(0)
count
local n_rows = r(N)

di as text "Benchmark: pq use_sas  fast (cached) vs  normal (streaming)"
di as text "File: `sas_file'"
di as text "Rows: `n_rows', Reps per cell: `reps'"
di as text ""
di as text "mode   | avg(s)   | speedup"
di as text "-------|----------|--------"

timer clear

// ── Normal (streaming) ──────────────────────────────────────────
forvalues r = 1/`reps' {
	clear
	timer on 1
	pq use_sas using "`sas_file'", clear auto_fast_limit(0)
	timer off 1
	assert _N == `n_rows'
}

// ── Fast (collect+cache) ─────────────────────────────────────────
forvalues r = 1/`reps' {
	clear
	timer on 2
	pq use_sas using "`sas_file'", clear fast
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
