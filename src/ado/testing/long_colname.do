set varabbrev off
set more off

// Regression test: parquet columns with names > 32 chars (Stata's variable
// name limit) must still load their *data*, not just get a truncated name.
//
// Bug: pq use correctly truncated the variable name and stored the original
// name in the {parquet_name:...} label, but the underlying data came back
// as all missing (numeric) / all empty (string) because the Rust read path
// looked up the Polars batch column by the *truncated* Stata name, which
// does not exist in the file, instead of the original parquet column name.
//
// Run long_colname_setup.py first to generate long_colname.parquet.

local f "long_colname.parquet"

pq use using "`f'", clear

assert _N == 3

// Truncated (32-char) variable names created by pq use
local num_var  this_numeric_column_name_is_long
local str_var  this_string_column_name_is_longe

capture confirm variable `num_var'
assert _rc == 0
capture confirm variable `str_var'
assert _rc == 0
capture confirm variable short_numeric
assert _rc == 0
capture confirm variable short_string
assert _rc == 0

// Original names preserved in the variable label
local numlbl : variable label `num_var'
assert "`numlbl'" == "{parquet_name:this_numeric_column_name_is_longer_than_32_chars}"
local strlbl : variable label `str_var'
assert "`strlbl'" == "{parquet_name:this_string_column_name_is_longer_than_32_chars}"

// The actual data must have loaded, not be all missing/empty
assert `num_var'[1] == 1
assert `num_var'[2] == 2
assert `num_var'[3] == 3

assert `str_var'[1] == "a"
assert `str_var'[2] == "b"
assert `str_var'[3] == "c"

// Short-named columns in the same file should be unaffected
assert short_numeric[1] == 10
assert short_numeric[2] == 20
assert short_numeric[3] == 30

assert short_string[1] == "x"
assert short_string[2] == "y"
assert short_string[3] == "z"

di as result "long_colname.do: PASSED"
