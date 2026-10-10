set varabbrev off

//	sort() with in() windows. Windowed sorts keep the first offset + n rows in a
//	bounded top-k buffer; full sorts run without a slice. Sorts are stable, so
//	tied rows keep file order.

local n_full = 20000

clear
set obs `n_full'
gen long id = _n
gen double x = runiform()
gen byte grp = mod(_n, 7)

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

//	Ties keep file order: grp == 0 holds ids 7, 14, ..., 19999 (2857 rows)
pq use "`pq_file'", clear sort(grp) in(1/50)
assert _N == 50
assert id == 7 * _n
di as text "stable ties, window at start: PASSED"

//	Window crossing from grp 0 into grp 1 (ids 1, 8, 15, ...)
pq use "`pq_file'", clear sort(grp) in(2851/2870)
assert _N == 20
gen long pos = 2850 + _n
assert id == 7 * pos if pos <= 2857
assert id == 1 + 7 * (pos - 2858) if pos > 2857
di as text "stable ties, window across groups: PASSED"

//	Full stable sort agrees with the windowed result
pq use "`pq_file'", clear sort(grp)
assert id == 7 * _n in 1/2857
assert id == 1 + 7 * (_n - 2858) in 2858/2870
di as text "stable ties, full sort: PASSED"

capture erase "`pq_file'"
di as result "All sort_window tests PASSED"
