set varabbrev off

//	Test that null/empty strL values round-trip correctly through parquet.
//	In Stata, missing strings are represented as ""; parquet stores them as null.
//	This test verifies the null→"" mapping in both directions.

tempfile pq1 pq2

//	----------------------------------------------------------------------
//	Test 1: strL-only column with mixed non-empty and empty values
//	----------------------------------------------------------------------
clear
set obs 6
gen strL longstr = ""
replace longstr = "x" * 3000   in 1	//	long value
replace longstr = ""            in 2	//	explicitly empty / missing
replace longstr = "y" * 3000   in 3	//	long value
replace longstr = ""            in 4	//	explicitly empty / missing
replace longstr = "hello"       in 5	//	short value in a strL column
replace longstr = ""            in 6	//	explicitly empty / missing

pq save "`pq1'.parquet", replace
pq use  "`pq1'.parquet", clear

assert _N == 6
assert strlen(longstr[1]) == 3000
assert missing(longstr[2])
assert strlen(longstr[3]) == 3000
assert missing(longstr[4])
assert longstr[5] == "hello"
assert missing(longstr[6])

//	Counts of missing and non-missing
quietly count if missing(longstr)
assert r(N) == 3
quietly count if !missing(longstr)
assert r(N) == 3

di as text "Test 1 (strL-only with nulls): PASSED"


//	----------------------------------------------------------------------
//	Test 2: mixed strL + numeric; nulls in strL must not corrupt numerics
//	----------------------------------------------------------------------
clear
set obs 5
gen long   id  = _n
gen double val = _n * 2.0
gen strL   lab = ""
replace lab = "a" * 3000   in 1
replace lab = ""            in 2	//	missing
replace lab = "b" * 3000   in 3
replace lab = ""            in 4	//	missing
replace lab = "c" * 3000   in 5

pq save "`pq2'.parquet", replace
pq use  "`pq2'.parquet", clear

assert _N == 5
assert id[1]  == 1  & id[5]  == 5
assert val[3] == 6.0

assert strlen(lab[1]) == 3000
assert missing(lab[2])
assert strlen(lab[3]) == 3000
assert missing(lab[4])
assert strlen(lab[5]) == 3000

quietly count if missing(lab)
assert r(N) == 2

di as text "Test 2 (mixed strL+numeric with nulls): PASSED"

di as result "All strL missing-value tests PASSED"
