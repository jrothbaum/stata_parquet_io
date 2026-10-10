set varabbrev off

//	Random sampling is applied as a streaming row-index filter (no full collect).
//	Checks: exact counts with if(), seed reproducibility, sort after sampling,
//	and hive-partitioned directory input.

local n_full = 20000
local seed   = 98765

clear
set obs `n_full'
gen long id = _n
gen byte grp = mod(_n, 4)
gen year = 2000 + mod(_n, 5)
gen double x = _n / 7

tempfile tbase
local pq_file "`tbase'.parquet"
local pq_hive "`tbase'_hive.parquet"
pq save "`pq_file'", replace
pq save "`pq_hive'", replace partition_by(year)

//	Filtered population: grp == 1 -> n_full/4 rows
local n_filtered = `n_full' / 4


//	----------------------------------------------------------------------
//	random_n + if: exact count, only filtered rows, distinct rows
//	----------------------------------------------------------------------
pq use "`pq_file'", clear if(grp == 1) random_n(500) random_seed(`seed')
assert _N == 500
assert grp == 1
quietly duplicates report id
assert r(unique_value) == 500
quietly sum id
local if_sum1 = r(sum)

pq use "`pq_file'", clear if(grp == 1) random_n(500) random_seed(`seed')
assert _N == 500
quietly sum id
assert r(sum) == `if_sum1'

pq use "`pq_file'", clear if(grp == 1) random_n(500) random_seed(`=`seed' + 1')
assert _N == 500
quietly sum id
assert r(sum) != `if_sum1'
di as text "random_n + if: PASSED"


//	----------------------------------------------------------------------
//	random_share + if: count matches floor(share * filtered rows)
//	----------------------------------------------------------------------
pq use "`pq_file'", clear if(grp == 1) random_share(0.3) random_seed(`seed')
assert _N == floor(0.3 * `n_filtered')
assert grp == 1
quietly duplicates report id
assert r(unique_value) == _N
di as text "random_share + if: PASSED"


//	----------------------------------------------------------------------
//	Sample rows come out in file order, and sort() applies after sampling
//	----------------------------------------------------------------------
pq use "`pq_file'", clear random_n(1000) random_seed(`seed')
assert _N == 1000
assert id > id[_n-1] if _n > 1
quietly sum id
local unsorted_sum = r(sum)

pq use "`pq_file'", clear random_n(1000) random_seed(`seed') sort(-id)
assert _N == 1000
assert id < id[_n-1] if _n > 1
quietly sum id
assert r(sum) == `unsorted_sum'
di as text "random_n + sort: PASSED"


//	----------------------------------------------------------------------
//	Batch size must not change which rows are drawn
//	----------------------------------------------------------------------
pq use "`pq_file'", clear random_n(1000) random_seed(`seed') batch_size(1000)
assert _N == 1000
assert id > id[_n-1] if _n > 1
quietly sum id
assert r(sum) == `unsorted_sum'

pq use "`pq_file'", clear if(grp == 1) random_n(500) random_seed(`seed') batch_size(1000)
assert _N == 500
quietly sum id
assert r(sum) == `if_sum1'
di as text "random_n + batch_size: PASSED"


//	----------------------------------------------------------------------
//	Full-population sample returns every row
//	----------------------------------------------------------------------
pq use "`pq_file'", clear if(grp == 1) random_share(1) random_seed(`seed')
assert _N == `n_filtered'
quietly sum id
local all_grp1_sum = r(sum)
pq use "`pq_file'", clear if(grp == 1)
quietly sum id
assert r(sum) == `all_grp1_sum'
di as text "random_share(1): PASSED"


//	----------------------------------------------------------------------
//	Hive-partitioned directory: if() alone and if() + random_n
//	----------------------------------------------------------------------
pq use "`pq_hive'", clear if(grp == 1)
assert _N == `n_filtered'
assert grp == 1

pq use "`pq_hive'", clear if(grp == 1) random_n(300) random_seed(`seed')
assert _N == 300
assert grp == 1
quietly duplicates report id
assert r(unique_value) == 300
quietly sum id
local hive_sum1 = r(sum)

pq use "`pq_hive'", clear if(grp == 1) random_n(300) random_seed(`seed')
assert _N == 300
quietly sum id
assert r(sum) == `hive_sum1'
di as text "hive + if + random_n: PASSED"


capture erase "`pq_file'"
di as result "All random_stream tests PASSED"
