set varabbrev off
clear

// ============================================================
// Generate test data: numeric + string + strL columns
// ============================================================
set seed 12345
set obs 100

gen long id = _n
gen double x = rnormal()
gen float y = runiform()
gen str10 name = "row" + string(_n)
gen strL long_text = ""
gen not_long = rnormal()
gen long_not = rnormal()
recast strL long_text
forvalues i = 1/100 {
	quietly replace long_text = long_text + "padding_text_to_make_this_very_long_"
}
quietly replace long_text = long_text + string(_n)

// Verify starting shape
assert _N == 100
describe

tempfile t_base

// Save to parquet
pq save "`t_base'.parquet", replace


// ============================================================
// TEST 1: pq use with drop() - exact variable name
// ============================================================
pq use using "`t_base'.parquet", clear drop(y)
assert _N == 100

// y should not exist
capture confirm variable y
assert _rc != 0

// other vars should exist
confirm variable id
confirm variable x
confirm variable name
confirm variable long_text
confirm variable not_long
confirm variable long_not

di "PASS: use with drop(y)"

// ============================================================
// TEST 2: pq use with drop() - wildcard pattern
// ============================================================
pq use using "`t_base'.parquet", clear drop(long*)
assert _N == 100

// long_text should not exist
capture confirm variable long_text
assert _rc != 0

// other vars should exist
confirm variable id
confirm variable x
confirm variable y
confirm variable name
confirm variable not_long

capture confirm variable long_not
assert _rc != 0

di "PASS: use with drop(long*)"


// ============================================================
// TEST 3: pq use with drop() - multiple variables
// ============================================================
pq use using "`t_base'.parquet", clear drop(y long_text)
assert _N == 100

capture confirm variable y
assert _rc != 0
capture confirm variable long_text
assert _rc != 0

confirm variable id
confirm variable x
confirm variable name

di "PASS: use with drop(y long_text)"


// ============================================================
// TEST 4: pq use with drop_strl
// ============================================================
pq use using "`t_base'.parquet", clear drop_strl
assert _N == 100

// long_text is strL so it should be dropped
capture confirm variable long_text
assert _rc != 0

// other vars should exist
confirm variable id
confirm variable x
confirm variable y
confirm variable name

di "PASS: use with drop_strl"


// ============================================================
// TEST 5: pq use with drop_strl and drop() combined
// ============================================================
pq use using "`t_base'.parquet", clear drop_strl drop(y)
assert _N == 100

capture confirm variable y
assert _rc != 0
capture confirm variable long_text
assert _rc != 0

confirm variable id
confirm variable x
confirm variable name

di "PASS: use with drop_strl drop(y)"


// ============================================================
// TEST 6: pq use with varlist AND drop()
// ============================================================
pq use id x y using "`t_base'.parquet", clear drop(y)
assert _N == 100

capture confirm variable y
assert _rc != 0
capture confirm variable name
assert _rc != 0
capture confirm variable long_text
assert _rc != 0

confirm variable id
confirm variable x

di "PASS: use with varlist and drop(y)"


// ============================================================
// TEST 7: pq use data integrity check after drop
// ============================================================
pq use using "`t_base'.parquet", clear drop(long_text y)
assert _N == 100
assert id[1] == 1
assert id[100] == 100
assert name[1] == "row1"
assert name[100] == "row100"

di "PASS: data integrity after drop"


// ============================================================
// TEST 8: pq append with drop()
// ============================================================
pq use using "`t_base'.parquet", clear drop(long_text)
local n_before = _N
pq append "`t_base'.parquet", drop(long_text)
assert _N == `n_before' * 2

capture confirm variable long_text
assert _rc != 0

confirm variable id
confirm variable x
confirm variable y
confirm variable name

// Check values are correct in both halves
assert id[1] == 1
assert id[`n_before'] == `n_before'
local idx = `n_before' + 1
assert id[`idx'] == 1

di "PASS: append with drop(long_text)"


// ============================================================
// TEST 9: pq append with drop_strl
// ============================================================
pq use using "`t_base'.parquet", clear drop_strl
local n_before = _N
pq append "`t_base'.parquet", drop_strl
assert _N == `n_before' * 2

capture confirm variable long_text
assert _rc != 0

confirm variable id
confirm variable x
confirm variable y
confirm variable name

di "PASS: append with drop_strl"


// ============================================================
// TEST 10: pq merge with drop()
// ============================================================

// Create a second file to merge on
clear
set obs 100
gen long id = _n
gen double z = rnormal()
gen strL merge_text = ""
recast strL merge_text
forvalues i = 1/100 {
	quietly replace merge_text = merge_text + "padding_text_to_make_this_very_long_"
}
quietly replace merge_text = merge_text + string(_n)

tempfile t_merge
pq save "`t_merge'.parquet", replace

// Load base and merge
pq use using "`t_base'.parquet", clear drop(long_text)
pq merge 1:1 id using "`t_merge'.parquet", nogenerate drop(merge_text)

assert _N == 100

// merge_text should not exist
capture confirm variable merge_text
assert _rc != 0

// z from merged file should exist
confirm variable z

// long_text should not exist (dropped on initial use)
capture confirm variable long_text
assert _rc != 0

di "PASS: merge with drop(merge_text)"


// ============================================================
// TEST 11: pq merge with drop_strl
// ============================================================
pq use using "`t_base'.parquet", clear drop_strl
pq merge 1:1 id using "`t_merge'.parquet", nogenerate drop_strl

assert _N == 100

// Both strL columns should be absent
capture confirm variable long_text
assert _rc != 0
capture confirm variable merge_text
assert _rc != 0

// Non-strL columns should exist
confirm variable id
confirm variable x
confirm variable y
confirm variable name
confirm variable z

di "PASS: merge with drop_strl"


// ============================================================
// TEST 12: drop with ? wildcard
// ============================================================
pq use using "`t_base'.parquet", clear drop(?)
assert _N == 100

// x and y are single-char names, should be dropped
capture confirm variable x
assert _rc != 0
capture confirm variable y
assert _rc != 0

confirm variable id
confirm variable name
confirm variable long_text

di "PASS: use with drop(?) wildcard"


// ============================================================
// Cleanup
// ============================================================
capture erase "`t_base'.parquet"
capture erase "`t_merge'.parquet"

di ""
di "ALL DROP/NOSTRL TESTS PASSED"
