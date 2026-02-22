set varabbrev off

//	Skip on non-Windows (path separator tests are Windows-specific)
if (c(os) != "Windows") {
	di as text "path_fixes.do: skipped (Windows-only test)"
	exit 0
}

local tmp_root C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\test_data\tmp
capture mkdir "`tmp_root'"
local test_abs_back `tmp_root'\pq_path_test.parquet
local test_abs_fwd  = subinstr("`test_abs_back'", "\", "/", .)

//	Reference dataset: row_id = _n (1-50), sum = 1275
clear
set obs 50
gen long row_id = _n
gen x = rnormal()
local ref_n      = 50
local ref_sum_id = 50 * 51 / 2

pq save "`test_abs_back'", replace


//	----------------------------------------------------------------------
//	Test 1: absolute path with Windows backslashes
//	----------------------------------------------------------------------
pq use "`test_abs_back'", clear

if _N != `ref_n' {
	di as error "Test 1: expected `ref_n' rows, got `=_N'"
	exit 9
}
quietly sum row_id
if r(sum) != `ref_sum_id' {
	di as error "Test 1: sum(row_id) `=r(sum)' != `ref_sum_id'"
	exit 9
}
di as text "Test 1 (absolute path, backslashes): PASSED"


//	----------------------------------------------------------------------
//	Test 2: absolute path with forward slashes
//	----------------------------------------------------------------------
pq use "`test_abs_fwd'", clear

if _N != `ref_n' {
	di as error "Test 2: expected `ref_n' rows, got `=_N'"
	exit 9
}
quietly sum row_id
if r(sum) != `ref_sum_id' {
	di as error "Test 2: sum(row_id) `=r(sum)' != `ref_sum_id'"
	exit 9
}
di as text "Test 2 (absolute path, forward slashes): PASSED"


//	----------------------------------------------------------------------
//	Test 3: cd to directory, load via relative filename
//	----------------------------------------------------------------------
local orig_dir = c(pwd)
quietly cd "`tmp_root'"

pq use "pq_path_test.parquet", clear

if _N != `ref_n' {
	quietly cd "`orig_dir'"
	di as error "Test 3: expected `ref_n' rows, got `=_N'"
	exit 9
}
quietly sum row_id
if r(sum) != `ref_sum_id' {
	quietly cd "`orig_dir'"
	di as error "Test 3: sum(row_id) `=r(sum)' != `ref_sum_id'"
	exit 9
}
quietly cd "`orig_dir'"
di as text "Test 3 (relative path after cd): PASSED"


//	----------------------------------------------------------------------
//	Test 4: pq path normalizes backslash and forward-slash paths identically
//	----------------------------------------------------------------------
pq path "`test_abs_back'"
local norm_back = r(fullpath)

pq path "`test_abs_fwd'"
local norm_fwd = r(fullpath)

//	pq path preserves slash style (backslash in → backslash out, fwd → fwd).
//	Compare after normalising both to forward slashes and lowercase.
local cmp_back = lower(subinstr("`norm_back'", "\", "/", .))
local cmp_fwd  = lower(subinstr("`norm_fwd'",  "\", "/", .))

if "`cmp_back'" == "" {
	di as error "Test 4: pq path returned empty for backslash input"
	exit 9
}
if "`cmp_back'" != "`cmp_fwd'" {
	di as error "Test 4: paths resolve to different locations:"
	di as error "  backslash form : `norm_back'"
	di as error "  fwd-slash form : `norm_fwd'"
	exit 9
}
di as text "Test 4 (pq path both slash styles resolve identically): PASSED"


capture erase "`test_abs_back'"

di as result "All path_fixes tests PASSED"
