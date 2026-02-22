set varabbrev off

//	Test that strL columns stay aligned with numeric columns when pq use
//	applies an if or in filter.  The key concern is that the _pq_strl_key
//	assigned by Rust matches _n in Stata after the filtered load.

tempfile pq1

//	Create dataset: id encodes the row number so we can verify alignment.
//	longstr encodes the row number in its content for the same reason.
clear
set obs 20
gen long   id      = _n
gen double val     = _n * 1.5
gen strL   longstr = "row_" + string(_n) + "_" + "x" * 3000

pq save "`pq1'.parquet", replace


//	----------------------------------------------------------------------
//	Test 1: if filter — load only rows where id > 10
//	----------------------------------------------------------------------
pq use "`pq1'.parquet", clear if(id > 10)

assert _N == 10
forvalues k = 1/10 {
	local expected_id = `k' + 10
	assert id[`k'] == `expected_id'
	assert val[`k'] == `expected_id' * 1.5
	//	strL prefix must match the expected id (use dynamic length)
	local exp_pfx = "row_" + string(`expected_id') + "_"
	assert substr(longstr[`k'], 1, length("`exp_pfx'")) == "`exp_pfx'"
}

di as text "Test 1 (if filter: id > 10): PASSED"


//	----------------------------------------------------------------------
//	Test 2: if filter with more selective condition — id in {3,7,15}
//	----------------------------------------------------------------------
pq use "`pq1'.parquet", clear if(inlist(id, 3, 7, 15))

assert _N == 3
assert id[1] == 3  & substr(longstr[1], 1, 6) == "row_3_"
assert id[2] == 7  & substr(longstr[2], 1, 6) == "row_7_"
assert id[3] == 15 & substr(longstr[3], 1, 7) == "row_15_"

di as text "Test 2 (if filter: inlist): PASSED"


//	----------------------------------------------------------------------
//	Test 3: in range filter — load only rows 5-14
//	----------------------------------------------------------------------
pq use "`pq1'.parquet", clear in(5/14)

assert _N == 10
forvalues k = 1/10 {
	local expected_id = `k' + 4
	local exp_pfx = "row_" + string(`expected_id') + "_"
	assert id[`k'] == `expected_id'
	assert substr(longstr[`k'], 1, length("`exp_pfx'")) == "`exp_pfx'"
}

di as text "Test 3 (in 5/14): PASSED"


//	----------------------------------------------------------------------
//	Test 4: pq append with if filter — rows 11-20 appended onto rows 1-10
//	----------------------------------------------------------------------
pq use "`pq1'.parquet", clear if(id <= 10)
assert _N == 10

pq append "`pq1'.parquet", if(id > 10)
assert _N == 20

//	First 10 rows: ids 1-10
forvalues k = 1/10 {
	local exp_pfx = "row_" + string(`k') + "_"
	assert id[`k'] == `k'
	assert substr(longstr[`k'], 1, length("`exp_pfx'")) == "`exp_pfx'"
}

//	Next 10 rows: ids 11-20
forvalues k = 1/10 {
	local r = `k' + 10
	local expected_id = `k' + 10
	local exp_pfx = "row_" + string(`expected_id') + "_"
	assert id[`r'] == `expected_id'
	assert substr(longstr[`r'], 1, length("`exp_pfx'")) == "`exp_pfx'"
}

di as text "Test 4 (pq append with if filter): PASSED"

di as result "All strL alignment (if/in) tests PASSED"
