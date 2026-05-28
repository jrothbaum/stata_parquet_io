set varabbrev off
set more off

// Test that format is inferred from file extension for pq use and pq save.

tempfile tparquet tcsv tsav

// Build reference dataset
clear
set obs 5
gen long   id  = _n
gen double x   = _n * 1.5
gen str10  lab = "row" + string(_n)


// --- Save tests: pq save auto-infers format from extension ---

pq save "`tparquet'.parquet", replace
di "PASS: pq save .parquet (no format() needed)"

pq save "`tcsv'.csv", replace
di "PASS: pq save .csv (no format() needed)"

pq save "`tsav'.sav", replace
di "PASS: pq save .sav (no format() needed)"


// --- Use tests: pq use auto-infers format from extension ---

pq use "`tparquet'.parquet", clear
assert _N == 5
di "PASS: pq use .parquet autoinferred as parquet"

pq use "`tcsv'.csv", clear
assert _N == 5
di "PASS: pq use .csv autoinferred as csv"

pq use "`tsav'.sav", clear
assert _N == 5
di "PASS: pq use .sav autoinferred as spss"


// --- SAS: read-only test if file is available ---
local sas_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\sas\data\data_poe\cars.sas7bdat"
capture confirm file "`sas_file'"
if _rc != 0 {
	di as text "note: SAS test file not found, skipping .sas7bdat autoinfer test"
}
else {
	pq use "`sas_file'", clear
	assert _N > 0
	di "PASS: pq use .sas7bdat autoinferred as sas"
}


// --- Explicit format() overrides extension ---
pq use "`tcsv'.csv", clear format(csv)
assert _N == 5
di "PASS: explicit format(csv) still works"


di as result _newline "All format autoinfer tests passed."
