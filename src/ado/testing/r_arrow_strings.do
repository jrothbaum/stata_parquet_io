// Test that pq save writes strings as Arrow large_string (not string_view)
// in the embedded ARROW:schema, so R's arrow package can read the files
// (apache/arrow#45523).  Writes the files read by r_arrow_strings.R and
// checks they still round-trip through pq use as strings.

local outdir "r_arrow_strings_out"
capture mkdir "`outdir'"

clear
set obs 100
gen long id = _n
gen str20 name = "name_" + string(_n)
gen strL note = "note " + string(_n) + " " + 50 * "x"
replace name = "" in 5
gen byte grp = mod(_n, 3)

// --- Test 1: plain save round-trips ---
pq save using "`outdir'/plain.parquet", replace
pq use using "`outdir'/plain.parquet", clear
assert _N == 100
confirm string variable name note
assert name == "name_" + string(id) if id != 5
assert name == "" if id == 5
assert note == "note " + string(id) + " " + 50 * "x"
di "PASS: plain save round-trips strings"

// --- Test 2: partitioned save round-trips ---
pq save using "`outdir'/partitioned", replace partition_by(grp)
pq use using "`outdir'/partitioned", clear
assert _N == 100
confirm string variable name note
di "PASS: partitioned save round-trips strings"

// --- Test 3: chunked + consolidated save round-trips ---
pq use using "`outdir'/plain.parquet", clear
pq save using "`outdir'/consolidated.parquet", replace chunk(30) consolidate
pq use using "`outdir'/consolidated.parquet", clear
assert _N == 100
confirm string variable name note
di "PASS: chunked/consolidated save round-trips strings"
