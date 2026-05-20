set varabbrev off

local f "cast_test.parquet"

//	cd "C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\src\ado\testing\"
// --- Test 1: binary_to_string converts binary column to string ---
pq use "`f'", clear binary_to_string
assert _N == 3

confirm string variable bin_col
di "bin_col[1] = >" bin_col[1] "<"
di "bin_col[2] = >" bin_col[2] "<"
di "bin_col[3] = >" bin_col[3] "<"
assert bin_col[1] == "hello"
assert bin_col[2] == "world"
assert bin_col[3] == "foo"
di "PASS: binary_to_string"


// --- Test 2: cast int64 -> int32 (long in Stata) ---
pq use "`f'", clear cast(`"{"int_col":"int32"}"')
assert _N == 3
local t: type int_col
assert "`t'" == "long"
assert int_col[1] == 100
di "PASS: cast int64->int32"


// --- Test 3: cast float64 -> float32 (float in Stata) ---
pq use "`f'", clear cast(`"{"float_col":"float32"}"')
assert _N == 3
local t: type float_col
assert "`t'" == "float"
di "PASS: cast float64->float32"


// --- Test 4: strict cast failure on non-numeric string -> rc != 0 ---
capture pq use "`f'", clear cast(`"{"str_col":"int32"}"')
assert _rc != 0
di "PASS: strict cast failure returns error"


// --- Test 5: lax cast of non-numeric string -> nulls ---
pq use "`f'", clear cast(`"{"str_col":"int32"}"') lax
assert _N == 3
assert missing(str_col[1])
di "PASS: lax cast produces nulls"


// --- Test 6: binary_to_string combined with cast on another column ---
pq use "`f'", clear binary_to_string cast(`"{"int_col":"int32"}"')
assert _N == 3
confirm string variable bin_col
assert bin_col[1] == "hello"
local t: type int_col
assert "`t'" == "long"
di "PASS: binary_to_string + cast combined"


// --- Test 7: append rollback on failed cast ---
pq use "`f'", clear
local n_before = _N
capture pq append "`f'", cast(`"{"str_col":"int32"}"')
assert _rc != 0
assert _N == `n_before'
di "PASS: append rollback on cast failure"


di "All cast tests passed."
