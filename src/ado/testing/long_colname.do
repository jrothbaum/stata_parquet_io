set varabbrev off
set more off

// Regression test: parquet columns with names > 32 chars (Stata's variable
// name limit) must still load their *data*, not just get a truncated name.
//
// Bug: pq use correctly truncated the variable name and stored the original
// name in the _pq_parquet_name characteristic, but the underlying data came
// back as all missing (numeric) / all empty (string) because the Rust read path
// looked up the Polars batch column by the *truncated* Stata name, which
// does not exist in the file, instead of the original parquet column name.
//
// Run long_colname_setup.py first to generate long_colname.parquet.

cd "C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\src\ado\testing"
local f "long_colname.parquet"
local f2 "long_colname2.parquet"


pq use using "`f'", clear

assert _N == 3

// Truncated (32-char) variable names created by pq use
local num_var  this_numeric_column_name_is__001 
local num_var2 this_numeric_column_name_is__002
local str_var  this_string_column_name_is_longe

capture confirm variable `num_var'
assert _rc == 0
capture confirm variable `num_var2'
assert _rc == 0
capture confirm variable `str_var'
assert _rc == 0
capture confirm variable short_numeric
assert _rc == 0
capture confirm variable short_string
assert _rc == 0

// Original names preserved in the _pq_parquet_name characteristic (not the
// variable label - moved there so a real variable label, e.g. one restored
// via statametadata, can coexist with the rename-on-save bookkeeping).
// num_var/num_var2 collide on their truncated prefix and get disambiguated
// with a _001/_002 suffix; which one gets which suffix is a deterministic
// function of file column order (see mapping.rs generate_rename_map) but
// this test doesn't assume a specific column order, so it checks the pair
// as a set instead of hardcoding which var gets which original name.
local numchar : char `num_var'[_pq_parquet_name]
local numchar2 : char `num_var2'[_pq_parquet_name]
local expect_a this_numeric_column_name_is_longer_than_32_chars
local expect_b this_numeric_column_name_is_longer_than_32_chars_too
assert inlist("`numchar'", "`expect_a'", "`expect_b'")
assert inlist("`numchar2'", "`expect_a'", "`expect_b'")
assert "`numchar'" != "`numchar2'"

local strchar : char `str_var'[_pq_parquet_name]
assert "`strchar'" == "this_string_column_name_is_longer_than_32_chars"

// The actual data must have loaded, not be all missing/empty
assert `num_var'[1] == 1
assert `num_var'[2] == 2
assert `num_var'[3] == 3

assert `num_var2'[1] == 3
assert `num_var2'[2] == 2
assert `num_var2'[3] == 1


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

pq save "`f2'", replace
pq use "`f2'", clear
assert `num_var'[1] == 1
assert `num_var'[2] == 2
assert `num_var'[3] == 3

assert `num_var2'[1] == 3
assert `num_var2'[2] == 2
assert `num_var2'[3] == 1


di as result "long_colname.do: PASSED"
