set varabbrev off

capture program drop create_data
program define create_data
	version 16
	syntax		, 	n_cols(integer)			///
					n_rows(integer)

	clear
	set obs `n_rows'
	local cols_created = 0

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen long c_`cols_created' = _n
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = char(65 + floor(runiform()*5))
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = rnormal()
		quietly tostring c_`cols_created', replace force
	}
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = floor(runiform()*100)
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_`ci' = rnormal()
		}
	}
end


local n_full  = 1001
local random_n     = 100
local random_share = 0.5
local seed         = 12345

set seed 20240101
create_data, n_rows(`n_full') n_cols(10)

tempfile pq_random
pq save "`pq_random'.parquet", replace


//	----------------------------------------------------------------------
//	Full load
//	----------------------------------------------------------------------
pq use "`pq_random'.parquet", clear

if _N != `n_full' {
	di as error "Full load: expected `n_full' rows, got `=_N'"
	exit 9
}
//	c_1 = _n, so sum = n*(n+1)/2
quietly sum c_1
local expected_sum_c1 = `n_full' * (`n_full' + 1) / 2
if r(sum) != `expected_sum_c1' {
	di as error "Full load: c_1 sum `=r(sum)' != `expected_sum_c1'"
	exit 9
}
di as text "Full load: PASSED (_N=`=_N')"


//	----------------------------------------------------------------------
//	random_n without seed — two calls must give different row sets
//	----------------------------------------------------------------------
pq use "`pq_random'.parquet", clear random_n(`random_n')
if _N != `random_n' {
	di as error "random_n (no seed): expected `random_n' rows, got `=_N'"
	exit 9
}
quietly sum c_1
local sum1 = r(sum)

pq use "`pq_random'.parquet", clear random_n(`random_n')
if _N != `random_n' {
	di as error "random_n (no seed, repeat): expected `random_n' rows, got `=_N'"
	exit 9
}
quietly sum c_1
local sum2 = r(sum)

//	Without a seed the two draws should almost certainly differ
if `sum1' == `sum2' {
	di as error "random_n (no seed): two draws produced identical row sets — suspicious"
	exit 9
}
di as text "random_n (no seed): PASSED (draw1 sum(c_1)=`sum1', draw2 sum(c_1)=`sum2')"


//	----------------------------------------------------------------------
//	random_n with seed — two calls must give the SAME row set
//	----------------------------------------------------------------------
pq use "`pq_random'.parquet", clear random_n(`random_n') random_seed(`seed')
if _N != `random_n' {
	di as error "random_n (seed): expected `random_n' rows, got `=_N'"
	exit 9
}
quietly sum c_1
local seed_sum1 = r(sum)

pq use "`pq_random'.parquet", clear random_n(`random_n') random_seed(`seed')
if _N != `random_n' {
	di as error "random_n (seed, repeat): expected `random_n' rows, got `=_N'"
	exit 9
}
quietly sum c_1
local seed_sum2 = r(sum)

if `seed_sum1' != `seed_sum2' {
	di as error "random_n (seed): draws differ: `seed_sum1' vs `seed_sum2'"
	exit 9
}
di as text "random_n (seed): PASSED (both draws sum(c_1)=`seed_sum1')"


//	----------------------------------------------------------------------
//	random_share without seed — two calls must give different row sets
//	----------------------------------------------------------------------
local expected_n_share = floor(`random_share' * `n_full')

pq use "`pq_random'.parquet", clear random_share(`random_share')
if _N != `expected_n_share' {
	di as error "random_share (no seed): expected `expected_n_share' rows, got `=_N'"
	exit 9
}
quietly sum c_1
local share_sum1 = r(sum)

pq use "`pq_random'.parquet", clear random_share(`random_share')
if _N != `expected_n_share' {
	di as error "random_share (no seed, repeat): expected `expected_n_share' rows, got `=_N'"
	exit 9
}
quietly sum c_1
local share_sum2 = r(sum)

if `share_sum1' == `share_sum2' {
	di as error "random_share (no seed): two draws produced identical row sets — suspicious"
	exit 9
}
di as text "random_share (no seed): PASSED (draw1 sum(c_1)=`share_sum1', draw2 sum(c_1)=`share_sum2')"


//	----------------------------------------------------------------------
//	random_share with seed — two calls must give the SAME row set
//	----------------------------------------------------------------------
pq use "`pq_random'.parquet", clear random_share(`random_share') random_seed(`seed')
if _N != `expected_n_share' {
	di as error "random_share (seed): expected `expected_n_share' rows, got `=_N'"
	exit 9
}
quietly sum c_1
local sseed_sum1 = r(sum)

pq use "`pq_random'.parquet", clear random_share(`random_share') random_seed(`seed')
if _N != `expected_n_share' {
	di as error "random_share (seed, repeat): expected `expected_n_share' rows, got `=_N'"
	exit 9
}
quietly sum c_1
local sseed_sum2 = r(sum)

if `sseed_sum1' != `sseed_sum2' {
	di as error "random_share (seed): draws differ: `sseed_sum1' vs `sseed_sum2'"
	exit 9
}
di as text "random_share (seed): PASSED (both draws sum(c_1)=`sseed_sum1')"


capture erase "`pq_random'.parquet"


//	----------------------------------------------------------------------
//	strL alignment: verify strl and non-strl columns select the same rows
//	----------------------------------------------------------------------
//	Encode the row number into the strL value so we can check alignment
//	after sampling: if the strL and numeric columns are from the same row,
//	strl_id must equal "row_" + string(c_1) for every observation.

set seed 20240101
create_data, n_rows(`n_full') n_cols(10)
gen strL strl_id = "row_" + string(c_1)

tempfile pq_strl
pq save "`pq_strl'.parquet", replace

capture program drop check_strl_alignment
program define check_strl_alignment
	args label
	gen byte __ok = (strl_id == "row_" + string(c_1))
	quietly sum __ok
	if r(min) != 1 {
		di as error "`label': strL misaligned on `=_N - r(sum)' row(s)"
		drop __ok
		exit 9
	}
	drop __ok
	di as text "`label': PASSED (_N=`=_N')"
end

//	Full load — baseline alignment
pq use "`pq_strl'.parquet", clear
check_strl_alignment "strL full load"

//	random_n with seed — alignment + reproducibility
pq use "`pq_strl'.parquet", clear random_n(`random_n') random_seed(`seed')
if _N != `random_n' {
	di as error "strL random_n: expected `random_n' rows, got `=_N'"
	exit 9
}
check_strl_alignment "strL random_n (seed, draw 1)"
quietly sum c_1
local strl_n_sum1 = r(sum)

pq use "`pq_strl'.parquet", clear random_n(`random_n') random_seed(`seed')
check_strl_alignment "strL random_n (seed, draw 2)"
quietly sum c_1
if r(sum) != `strl_n_sum1' {
	di as error "strL random_n (seed): draws differ: `strl_n_sum1' vs `=r(sum)'"
	exit 9
}
di as text "strL random_n seed reproducibility: PASSED"

//	random_share with seed — alignment + reproducibility
pq use "`pq_strl'.parquet", clear random_share(`random_share') random_seed(`seed')
if _N != `expected_n_share' {
	di as error "strL random_share: expected `expected_n_share' rows, got `=_N'"
	exit 9
}
check_strl_alignment "strL random_share (seed, draw 1)"
quietly sum c_1
local strl_share_sum1 = r(sum)

pq use "`pq_strl'.parquet", clear random_share(`random_share') random_seed(`seed')
check_strl_alignment "strL random_share (seed, draw 2)"
quietly sum c_1
if r(sum) != `strl_share_sum1' {
	di as error "strL random_share (seed): draws differ: `strl_share_sum1' vs `=r(sum)'"
	exit 9
}
di as text "strL random_share seed reproducibility: PASSED"

capture erase "`pq_strl'.parquet"

di as result "All random read tests PASSED"
