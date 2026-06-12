set varabbrev off

//	Regression test: relaxed mode must upcast across hive partitions with
//	differing integer widths (Int32 in a=1, Int16 in a=2).
//
//	Setup script: src/ado/testing/hive_relaxed_setup.py

local hive_root C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\test_data\hive_relaxed
local glob_pat  `"`hive_root'\*\*.parquet"'

// --- Test 1: relaxed load should succeed and return all 6 rows ---
pq use `glob_pat', clear relaxed

assert _N == 6
di as text "Test 1 (row count): PASSED (_N = `=_N')"

// --- Test 2: 'index' column present and has no missing values ---
confirm numeric variable index
quietly count if missing(index)
assert r(N) == 0
di as text "Test 2 (no missing in index): PASSED"

// --- Test 3: values are correct (1..6 in partition order) ---
quietly sum index
assert r(sum) == 21   // 1+2+3+4+5+6
di as text "Test 3 (sum of index == 21): PASSED"

di as result "All hive_relaxed glob tests PASSED"

// --- Test 4: directory path with relaxed should also upcast correctly ---
pq use `"`hive_root'"', clear relaxed

assert _N == 6
di as text "Test 4 (directory path, row count): PASSED (_N = `=_N')"

confirm numeric variable index
quietly count if missing(index)
assert r(N) == 0
di as text "Test 5 (directory path, no missing): PASSED"

quietly sum index
assert r(sum) == 21
di as text "Test 6 (directory path, sum of index == 21): PASSED"

di as result "All hive_relaxed tests PASSED"
