set varabbrev off

//	Regression test: pq save <varlist> must write each named variable's data,
//	not the data of the variable at the same positional index in the dataset.

tempfile f0 f1 f2 f3

clear
set obs 5
gen long a = _n            // 1 2 3 4 5
gen long b = _n * 10       // 10 20 30 40 50
gen long c = _n * 100      // 100 200 300 400 500
gen str3 s = "S" + string(_n)


// --- Case 0 (control): full save round-trips correctly ---
pq save using "`f0'.parquet", replace
pq use "`f0'.parquet", clear
assert _N == 5
assert a[1] == 1  & a[5] == 5
assert b[1] == 10 & b[5] == 50
assert c[1] == 100 & c[5] == 500
assert s[1] == "S1" & s[5] == "S5"
di as text "Case 0 (full save round-trip): PASSED"


// --- Case 1: single non-first variable ---
clear
set obs 5
gen long a = _n
gen long b = _n * 10
gen long c = _n * 100
gen str3 s = "S" + string(_n)

pq save c using "`f1'.parquet", replace
pq use "`f1'.parquet", clear
assert _N == 5
assert c[1] == 100
assert c[2] == 200
assert c[5] == 500
di as text "Case 1 (single non-first var): PASSED"


// --- Case 2: two-variable non-prefix subset ---
clear
set obs 5
gen long a = _n
gen long b = _n * 10
gen long c = _n * 100
gen str3 s = "S" + string(_n)

pq save b c using "`f2'.parquet", replace
pq use "`f2'.parquet", clear
assert _N == 5
assert b[1] == 10  & b[5] == 50
assert c[1] == 100 & c[5] == 500
di as text "Case 2 (two-var non-prefix subset): PASSED"


// --- Case 3: string variable after numeric vars ---
clear
set obs 5
gen long a = _n
gen long b = _n * 10
gen long c = _n * 100
gen str3 s = "S" + string(_n)

pq save a s using "`f3'.parquet", replace
pq use "`f3'.parquet", clear
assert _N == 5
assert a[1] == 1  & a[5] == 5
assert s[1] == "S1" & s[5] == "S5"
di as text "Case 3 (string var after numeric): PASSED"


di as result "All save_varlist tests PASSED"
