set varabbrev off

//	Self-contained strL round-trip test.
//	Generates its own data so it runs on any machine.

tempfile pq1 pq2

//	----------------------------------------------------------------------
//	Test 1: basic strL save + use round-trip
//	----------------------------------------------------------------------
clear
set obs 5
gen strL longstr = ""
replace longstr = "hello"                       in 1	//	short value in strL column
replace longstr = "A_B_test"                    in 2	//	another short value
replace longstr = "x" * 3000 + "_end"          in 3	//	truly long (> 2045 bytes)
replace longstr = "y" * 2046                   in 4	//	just over the str# threshold
replace longstr = ""                            in 5	//	empty / missing

pq save "`pq1'.parquet", replace
pq use  "`pq1'.parquet", clear

assert _N == 5
assert longstr[1] == "hello"
assert longstr[2] == "A_B_test"
assert strlen(longstr[3]) == 3004
assert substr(longstr[3], 3001, 4) == "_end"
assert strlen(longstr[4]) == 2046
assert longstr[5] == ""

di as text "Test 1 (basic strL round-trip): PASSED"


//	----------------------------------------------------------------------
//	Test 2: mixed strL + numeric round-trip
//	----------------------------------------------------------------------
clear
set obs 4
gen long id   = _n
gen double val = _n * 1.5
gen strL label = ""
replace label = "alpha"            in 1
replace label = "beta"             in 2
replace label = "x" * 3000        in 3
replace label = "delta"            in 4

pq save "`pq2'.parquet", replace
pq use  "`pq2'.parquet", clear

assert _N == 4
assert id[1] == 1 & id[4] == 4
assert val[2] == 3.0
assert label[1] == "alpha"
assert label[2] == "beta"
assert strlen(label[3]) == 3000
assert label[4] == "delta"

di as text "Test 2 (mixed strL + numeric round-trip): PASSED"

di as result "All strL tests PASSED"
