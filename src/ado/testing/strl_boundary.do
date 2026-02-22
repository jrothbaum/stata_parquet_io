set varabbrev off

//	Test the 2045-byte strL promotion threshold.
//	pq.ado promotes a parquet string column to strL when its max length > 2045.
//	Stata's maximum fixed-length string type is str2045.
//
//	Scenarios:
//	  A) str2044 column  → max parquet len 2044 → loads as str2044 (not promoted)
//	  B) str2045 column  → max parquet len 2045 → loads as str2045 (not promoted)
//	  C) strL column (len 2046) → max parquet len 2046 → loads as strL (promoted)
//	  D) strL column (len 3000) → well above threshold → loads as strL

tempfile pq1

//	----------------------------------------------------------------------
//	Build dataset with columns at and around the threshold
//	----------------------------------------------------------------------
clear
set obs 3

gen str2044 col_2044 = "a" * 2044
gen str2045 col_2045 = "b" * 2045
gen strL    col_2046 = "c" * 2046
gen strL    col_3000 = "d" * 3000

//	Mix in shorter strings in later rows to confirm max drives the type
replace col_2046 = "short"   in 2
replace col_3000 = "shorter" in 3

pq save "`pq1'.parquet", replace
pq use  "`pq1'.parquet", clear

assert _N == 3

//	--- Column A: str2044 → should not be promoted (max len = 2044 ≤ 2045) ---
local type_2044 : type col_2044
if (lower("`type_2044'") == "strl") {
	di as error "col_2044: expected str2044, got strL — FAILED"
	exit 9
}
assert strlen(col_2044[1]) == 2044
di as text "col_2044 type=`type_2044': PASSED (not promoted)"

//	--- Column B: str2045 → should not be promoted (max len = 2045 = threshold) ---
local type_2045 : type col_2045
if (lower("`type_2045'") == "strl") {
	di as error "col_2045: expected str2045, got strL — FAILED"
	exit 9
}
assert strlen(col_2045[1]) == 2045
di as text "col_2045 type=`type_2045': PASSED (not promoted)"

//	--- Column C: strL 2046 → must be promoted / stay strL (max len = 2046 > 2045) ---
local type_2046 : type col_2046
if (lower("`type_2046'") != "strl") {
	di as error "col_2046: expected strL, got `type_2046' — FAILED"
	exit 9
}
assert strlen(col_2046[1]) == 2046
assert col_2046[2] == "short"
di as text "col_2046 type=`type_2046': PASSED (promoted to strL)"

//	--- Column D: strL 3000 → well above threshold → strL ---
local type_3000 : type col_3000
if (lower("`type_3000'") != "strl") {
	di as error "col_3000: expected strL, got `type_3000' — FAILED"
	exit 9
}
assert strlen(col_3000[1]) == 3000
assert col_3000[3] == "shorter"
di as text "col_3000 type=`type_3000': PASSED (strL)"

di as result "All strL boundary tests PASSED"
