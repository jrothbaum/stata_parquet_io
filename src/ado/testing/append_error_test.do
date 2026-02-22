set varabbrev off

tempfile pq1 pq2 f_append1 f_append2 f_append3

local with_long_string = 1

sysuse sp500, clear
gen nr = _n

//	sp500 has 248 observations.  keep if _n < 5 → rows 1-4,
//	keep if _n > 244 → rows 245-248.  After append: 8 rows.
//	Compute expected stats from the actual data — robust to dataset changes.
preserve
	keep if _n < 5 | _n > 244
	local expected_n = _N
	quietly sum nr
	local expected_sum_nr = r(sum)
	quietly sum open
	local expected_sum_open = r(sum)
	quietly sum high
	local expected_sum_high = r(sum)
restore

local keep_long_string
if (`with_long_string') {
	gen long_string = string(_n) + 4000*"a"
	local keep_long_string long_string
}

//	Create two non-overlapping parquet files
preserve
	keep if _n < 5
	pq save using "`pq1'.parquet", replace
restore

preserve
	keep if _n > 244
	pq save using "`pq2'.parquet", replace
restore


//	----------------------------------------------------------------------
//	Test 1: pq use with explicit varlist + pq append with explicit varlist
//	----------------------------------------------------------------------
pq use nr date open high volume `keep_long_string' using "`pq1'.parquet", clear
pq append nr date open high volume `keep_long_string' using "`pq2'.parquet"
list

order nr date open high volume `keep_long_string'
if (`with_long_string') replace long_string = substr(long_string, 1, 10)
save "`f_append1'.dta", replace

if _N != `expected_n' {
	di as error "Test 1: expected `expected_n' rows, got `=_N'"
	exit 9
}
quietly sum nr
if r(sum) != `expected_sum_nr' {
	di as error "Test 1: sum(nr) `=r(sum)' != `expected_sum_nr'"
	exit 9
}
quietly sum open
local t1_sum_open = r(sum)
quietly sum high
local t1_sum_high = r(sum)
di as text "Test 1 (explicit varlist use + append): PASSED (_N=`=_N', sum(nr)=`=r(sum)-r(sum)+`expected_sum_nr'')"


//	----------------------------------------------------------------------
//	Test 2: pq use all variables + pq append with explicit varlist
//	----------------------------------------------------------------------
pq use using "`pq1'.parquet", clear
pq append nr date open high volume `keep_long_string' using "`pq2'.parquet"
keep nr date open high volume `keep_long_string'
order nr date open high volume `keep_long_string'
if (`with_long_string') replace long_string = substr(long_string, 1, 10)
save "`f_append2'.dta", replace

if _N != `expected_n' {
	di as error "Test 2: expected `expected_n' rows, got `=_N'"
	exit 9
}
quietly sum nr
if r(sum) != `expected_sum_nr' {
	di as error "Test 2: sum(nr) `=r(sum)' != `expected_sum_nr'"
	exit 9
}
quietly sum open
if abs(r(sum) - `t1_sum_open') > 0.01 {
	di as error "Test 2: sum(open) `=r(sum)' differs from Test 1 (`t1_sum_open')"
	exit 9
}
di as text "Test 2 (pq use all + explicit append): PASSED"


//	----------------------------------------------------------------------
//	Test 3: pq use all variables + pq append all variables
//	----------------------------------------------------------------------
pq use using "`pq1'.parquet", clear
pq append using "`pq2'.parquet"
keep nr date open high volume `keep_long_string'
order nr date open high volume `keep_long_string'
if (`with_long_string') replace long_string = substr(long_string, 1, 10)
save "`f_append3'.dta", replace

if _N != `expected_n' {
	di as error "Test 3: expected `expected_n' rows, got `=_N'"
	exit 9
}
quietly sum nr
if r(sum) != `expected_sum_nr' {
	di as error "Test 3: sum(nr) `=r(sum)' != `expected_sum_nr'"
	exit 9
}
quietly sum open
if abs(r(sum) - `t1_sum_open') > 0.01 {
	di as error "Test 3: sum(open) `=r(sum)' differs from Test 1 (`t1_sum_open')"
	exit 9
}
quietly sum high
if abs(r(sum) - `t1_sum_high') > 0.01 {
	di as error "Test 3: sum(high) `=r(sum)' differs from Test 1 (`t1_sum_high')"
	exit 9
}
di as text "Test 3 (pq use all + full append): PASSED"


//	----------------------------------------------------------------------
//	Consistency: all three methods produce the same dataset
//	----------------------------------------------------------------------
//	Capture key sums from f_append1
use "`f_append1'.dta", clear
quietly sum nr
local a1_sum_nr = r(sum)
quietly sum open
local a1_sum_open = r(sum)
quietly sum high
local a1_sum_high = r(sum)

use "`f_append2'.dta", clear
quietly sum nr
if r(sum) != `a1_sum_nr' { 
	di as error "Consistency: f_append2 sum(nr) differs"
	exit 9 
}
quietly sum open
if abs(r(sum) - `a1_sum_open') > 0.01 { 
	di as error "Consistency: f_append2 sum(open) differs"
	exit 9 
}

use "`f_append3'.dta", clear
quietly sum nr
if r(sum) != `a1_sum_nr' { 
	di as error "Consistency: f_append3 sum(nr) differs"
	exit 9 
}
quietly sum open
if abs(r(sum) - `a1_sum_open') > 0.01 { 
	di as error "Consistency: f_append3 sum(open) differs"
	exit 9 
}
quietly sum high
if abs(r(sum) - `a1_sum_high') > 0.01 { 
	di as error "Consistency: f_append3 sum(high) differs"
	exit 9
}

di as text "Consistency (all three append methods agree): PASSED"


di as result "All append_error tests PASSED"
