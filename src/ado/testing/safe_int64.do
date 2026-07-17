set varabbrev off

local f "safe_int64_test.parquet"

//	cd "C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\src\ado\testing\"

// --- Test 1: default behavior errors out on Int64 precision overflow ---
capture pq use "`f'", clear
assert _rc != 0
di "PASS: default errors on Int64 precision overflow"


// --- Test 2: safe_int64 auto-loads the overflowing column as a string, preserving distinct values ---
pq use "`f'", clear safe_int64
assert _N == 5
confirm string variable big_id
assert big_id[1] == "9100000000000000001"
assert big_id[2] == "9100000000000000002"
assert big_id[3] == "9100000000000000003"
assert big_id[4] == "9100000000000000004"
assert big_id[5] == "9100000000000000005"
quietly levelsof big_id, local(levels)
assert r(r) == 5
di "PASS: safe_int64 loads big_id as string with all 5 distinct values"

// small_id is untouched (still numeric, not promoted to string)
local t: type small_id
assert "`t'" == "double"
assert small_id[1] == 1
di "PASS: safe_int64 leaves in-range Int64 columns numeric"


// --- Test 3: explicit cast() to string on the affected column avoids the error too ---
pq use "`f'", clear cast(`"{"big_id":"string"}"')
assert _N == 5
confirm string variable big_id
assert big_id[3] == "9100000000000000003"
di "PASS: explicit cast(string) on the overflowing column avoids the error"


di "All safe_int64 tests passed."
