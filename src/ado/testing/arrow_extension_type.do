// Test that parquet files with Arrow extension types (e.g. R haven_labelled /
// arrow.r.vctrs) load without panicking and produce the correct numeric column.

local f "repro_arrow_r_vctrs.parquet"

// --- Test 1: pq use loads the file and sexo is numeric ---
pq use "`f'", clear
assert _N > 0
confirm numeric variable sexo
di "PASS: arrow extension type column loaded as numeric"

// --- Test 2: values are in the expected range (1=Masculino, 2=Femenino) ---
assert inlist(sexo, 1, 2)
di "PASS: sexo values are 1 or 2"

// --- Test 3: pq describe does not panic ---
pq describe "`f'"
di "PASS: pq describe with arrow extension type"
