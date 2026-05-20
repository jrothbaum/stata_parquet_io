set varabbrev off

local f "cast_test.sav"


// --- Test 1: cast float64 -> int32 (strict, whole numbers so should succeed) ---
pq use_spss "`f'", clear cast(`"{"num_col":"int32"}"')
assert _N == 3
local t: type num_col
assert "`t'" == "long"
assert num_col[1] == 10
assert num_col[2] == 20
assert num_col[3] == 30
di "PASS SPSS: strict cast float64->int32"


// --- Test 2: lax cast of str_col -> int32 (two valid, one non-numeric -> null) ---
pq use_spss "`f'", clear cast(`"{"str_col":"int32"}"') lax
assert _N == 3
local t: type str_col
assert "`t'" == "long"
assert str_col[1] == 100
assert str_col[2] == 200
assert missing(str_col[3])
di "PASS SPSS: lax cast str->int32 (null for non-numeric)"


// --- Test 3: strict cast of str_col -> int32 must fail ---
capture pq use_spss "`f'", clear cast(`"{"str_col":"int32"}"')
assert _rc != 0
di "PASS SPSS: strict cast on mixed string column fails"


di "All SPSS cast tests passed."
