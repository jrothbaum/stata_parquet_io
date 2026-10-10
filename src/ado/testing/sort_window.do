set varabbrev off

//	sort() with in() windows. Small windows at the start run as a polars top-k;
//	other windows sort everything and are cut as batches are written.

local n_full = 20000

clear
set obs `n_full'
gen long id = _n
gen double x = runiform()

tempfile tbase
local pq_file "`tbase'.parquet"
pq save "`pq_file'", replace


//	Full sort
pq use "`pq_file'", clear sort(-id)
assert _N == `n_full'
assert id == `n_full' - _n + 1
di as text "sort full: PASSED"

//	Window at the start (top-k path)
pq use "`pq_file'", clear sort(-id) in(1/10)
assert _N == 10
assert id == `n_full' - _n + 1
di as text "sort in(1/10): PASSED"

//	Window in the middle (sorted, cut in callback)
pq use "`pq_file'", clear sort(-id) in(5001/5010)
assert _N == 10
assert id == `n_full' - 5000 - _n + 1
di as text "sort in(5001/5010): PASSED"

//	Large window at the start (sorted, cut in callback)
pq use "`pq_file'", clear sort(-id) in(1/15000)
assert _N == 15000
assert id == `n_full' - _n + 1
di as text "sort in(1/15000): PASSED"

//	Ascending sort with if() and a window
pq use "`pq_file'", clear sort(id) if(id > 100) in(11/20)
assert _N == 10
assert id == 110 + _n
di as text "sort + if + in: PASSED"

capture erase "`pq_file'"
di as result "All sort_window tests PASSED"
