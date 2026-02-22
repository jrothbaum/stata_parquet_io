set more off

// Tests pq append when the parquet file has ONLY strL columns (no numeric columns).
// This exercises the all-strL append code path (n_matched_vars == 0 in pq_use_append).

tempfile part1 part2

// Part 1: 3 rows, strL column only
clear
set obs 3
gen strL longstr = ""
replace longstr = "a" * 3000 + "hello" in 1
replace longstr = "b" * 3000 + "hell"  in 2
replace longstr = "c" * 3000 + "hel"   in 3
pq save "`part1'.parquet", replace

// Part 2: 2 rows, strL column only (different content)
clear
set obs 2
gen strL longstr = ""
replace longstr = "x" * 2000 + "world" in 1
replace longstr = "y" * 2000 + "earth" in 2
pq save "`part2'.parquet", replace

// -----------------------------------------------------------------------
// Test 1: pq use of strL-only file (already covered by debug_strl_save,
//         included here as baseline before append)
// -----------------------------------------------------------------------
pq use "`part1'.parquet", clear
assert _N == 3
assert length(longstr[1]) == 3005
assert length(longstr[2]) == 3004
assert length(longstr[3]) == 3003
assert substr(longstr[1], length(longstr[1])-4, 5) == "hello"
assert substr(longstr[2], length(longstr[2])-3, 4) == "hell"
assert substr(longstr[3], length(longstr[3])-2, 3) == "hel"

// -----------------------------------------------------------------------
// Test 2: pq append of a strL-only file onto a strL-only loaded dataset
// -----------------------------------------------------------------------
pq append "`part2'.parquet"
assert _N == 5

// Original rows unchanged
assert substr(longstr[1], length(longstr[1])-4, 5) == "hello"
assert substr(longstr[2], length(longstr[2])-3, 4) == "hell"
assert substr(longstr[3], length(longstr[3])-2, 3) == "hel"

// Appended rows present and correct
assert length(longstr[4]) == 2005
assert length(longstr[5]) == 2005
assert substr(longstr[4], length(longstr[4])-4, 5) == "world"
assert substr(longstr[5], length(longstr[5])-4, 5) == "earth"
