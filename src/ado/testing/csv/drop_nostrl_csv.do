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
pq save_csv "`t_base'.csv", replace


// ============================================================
// TEST 1: pq use_csv with drop() - exact variable name
// ============================================================
pq use_csv using "`t_base'.csv", clear drop(y)
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
// TEST 2: pq use_csv with drop() - wildcard pattern
// ============================================================
pq use_csv using "`t_base'.csv", clear drop(long*)
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
// TEST 3: pq use_csv with drop() - multiple variables
// ============================================================
pq use_csv using "`t_base'.csv", clear drop(y long_text)
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
// TEST 4: pq use_csv with drop_strl
// ============================================================
pq use_csv using "`t_base'.csv", clear drop_strl
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
// TEST 5: pq use_csv with drop_strl and drop() combined
// ============================================================
pq use_csv using "`t_base'.csv", clear drop_strl drop(y)
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
// TEST 6: pq use_csv with varlist AND drop()
// ============================================================
pq use_csv id x y using "`t_base'.csv", clear drop(y)
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
// TEST 7: pq use_csv data integrity check after drop
// ============================================================
pq use_csv using "`t_base'.csv", clear drop(long_text y)
assert _N == 100
assert id[1] == 1
assert id[100] == 100
assert name[1] == "row1"
assert name[100] == "row100"

di "PASS: data integrity after drop"


// ============================================================
// TEST 8: pq append with drop()
// ============================================================
pq use_csv using "`t_base'.csv", clear drop(long_text)
local n_before = _N
pq append "`t_base'.csv", drop(long_text) format(csv)
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
pq use_csv using "`t_base'.csv", clear drop_strl
local n_before = _N
pq append "`t_base'.csv", drop_strl format(csv)
assert _N == `n_before' * 2

capture confirm variable long_text
assert _rc != 0

confirm variable id
confirm variable x
confirm variable y
confirm variable name

di "PASS: append with drop_strl"


// ============================================================
// NOTE: merge coverage for CSV is in use_options_csv.do.
// ============================================================


// ============================================================
// TEST 12: drop with ? wildcard
// ============================================================
pq use_csv using "`t_base'.csv", clear drop(?)
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
capture erase "`t_base'.csv"

di ""
di "ALL DROP/NOSTRL TESTS PASSED"


