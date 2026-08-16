set more off
set varabbrev off

//	Validates two related mechanisms:
//	 (a) `pq use` on Parquet (no compress) now sizes integer columns from
//	     Parquet row-group footer statistics by default - cheap (footer
//	     only, no data scan), and replaces the old conservative "always
//	     upcast one level" default whenever those stats are trustworthy.
//	 (b) `pq save ..., statametadata` additionally records each variable's
//	     exact Stata storage type, so a variable deliberately declared WIDER
//	     than its current values strictly need (e.g. kept `int` in
//	     anticipation of future appends, even though this save's data would
//	     fit `byte`) round-trips at its declared width, not narrowed to
//	     whatever the immediate data alone would justify.
//	Both are safe by construction: the recorded/declared type can only
//	WIDEN what footer stats alone would give, never narrow below it - never
//	risking data loss on roundtrip.

tempfile plain_pq meta_pq
local union_dir "`c(tmpdir)'pq_type_roundtrip_dir"
//	Idempotent: a prior run that errored out before reaching cleanup at the
//	bottom can leave this directory (and its files) behind, which would
//	make `mkdir' below fail on a rerun - start from a clean slate.
capture erase "`union_dir'/a.parquet"
capture erase "`union_dir'/b.parquet"
capture rmdir "`union_dir'"

//	--- 1. Footer-stats-only tightening: no statametadata needed - a plain
//	save/use round trip now returns the tight type straight from the
//	file's own footer statistics. ---
clear
set obs 5
gen byte x = mod(_n, 50)          // 1-5, well inside Stata byte's range
pq save "`plain_pq'.parquet", replace
pq use "`plain_pq'.parquet", clear
assert "`:type x'" == "byte"
assert x[1] == 1 & x[5] == 5
di as result "1. footer-stats-only: byte data returns as byte, no metadata needed - PASSED"

//	--- 2. statametadata preserves a WIDER declared type than the data
//	alone needs: x is declared `int` even though its values would fit
//	`byte` - recorded metadata keeps it at `int` on read, rather than
//	narrowing to whatever footer stats alone would justify. ---
clear
set obs 5
gen int x = mod(_n, 50)           // declared wider than the data requires
pq save "`meta_pq'.parquet", replace statametadata
pq use "`meta_pq'.parquet", clear
assert "`:type x'" == "int"
assert x[1] == 1 & x[5] == 5
di as result "2. statametadata: declared-wide int preserved, not narrowed to byte - PASSED"

//	--- 3. compress fix: narrowing a plain (no-metadata) read now reports
//	the narrow type too, not one level wider than what compress computed ---
clear
pq use "`plain_pq'.parquet", clear compress
assert "`:type x'" == "byte"
assert x[1] == 1 & x[5] == 5
di as result "3. compress: byte data now reports as byte (was int) - PASSED"

//	--- 4. Safety net: a directory glob spanning two files with very
//	different actual ranges (one byte-sized, one needing int) must never
//	lose data - the union's real range always wins. These two files record
//	genuinely different stata_type metadata (byte vs int), which pq.ado's
//	pre-existing pre-flight check refuses to load at all unless the caller
//	opts out with nostatametadata (a separate, stronger guard than
//	anything added here) - passing it exercises the footer-stats-only
//	path's OWN multi-file aggregation as the sole safety net. ---
mkdir "`union_dir'"
clear
set obs 5
gen byte x = mod(_n, 50)                   // 1-5 -> byte
pq save "`union_dir'/a.parquet", replace statametadata
clear
set obs 5
gen int x = 5000 + _n                      // 5001-5005 -> needs int, not byte
pq save "`union_dir'/b.parquet", replace statametadata

pq use "`union_dir'/*.parquet", clear nostatametadata
assert _N == 10
assert "`:type x'" != "byte"
quietly count if missing(x)
assert r(N) == 0
quietly summarize x
assert r(min) == 1
assert r(max) == 5005
di as result "4. multi-file union (footer-stats only): type widens to cover both files, no data loss - PASSED"

capture erase "`plain_pq'.parquet"
capture erase "`meta_pq'.parquet"
capture erase "`union_dir'/a.parquet"
capture erase "`union_dir'/b.parquet"
capture rmdir "`union_dir'"

di as result _newline "All type_roundtrip tests PASSED"
