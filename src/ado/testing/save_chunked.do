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
		quietly gen c_`cols_created' = _n
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = char(65 + floor(runiform()*5))
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen int year = floor(runiform()*5) + 2017
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen int month = floor(runiform()*12) + 1
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_`ci' = rnormal()
		}
	}
end

capture program drop assert_stats
program define assert_stats
	//	Check row count and key sums against stored reference values
	syntax, n(integer) sum_c1(real) sum_year(real) sum_c5(real) label(string)

	if _N != `n' {
		di as error "`label': expected `n' rows, got `=_N'"
		exit 9
	}

	quietly sum c_1
	if abs(r(sum) - `sum_c1') > 0.5 {
		di as error "`label': c_1 sum `=r(sum)' != expected `sum_c1'"
		exit 9
	}

	quietly sum year
	if abs(r(sum) - `sum_year') > 0.5 {
		di as error "`label': year sum `=r(sum)' != expected `sum_year'"
		exit 9
	}

	//	c_5 is float so allow small relative tolerance
	quietly sum c_5
	local tol = max(1, abs(`sum_c5')) * 1e-6
	if abs(r(sum) - `sum_c5') > `tol' {
		di as error "`label': c_5 sum `=r(sum)' != expected `sum_c5' (tol=`tol')"
		exit 9
	}

	//	c_2 must only contain A-E
	quietly tab c_2
	if r(r) > 5 {
		di as error "`label': c_2 has `=r(r)' distinct values, expected <= 5"
		exit 9
	}

	di as text "`label': PASSED (_N=`=_N')"
end


//	Use Stata tempfile for a portable base path; .parquet suffix added below
tempfile tparquet

//	Create reference data with fixed seed
set seed 20240101
create_data, n_rows(10000) n_cols(6)

//	Confirm structure and store reference statistics
assert _N == 10000
confirm variable c_1 c_2 year month c_5 c_6

//	c_1 = _n so sum is exact: 10000*10001/2 = 50005000
quietly sum c_1
assert r(sum) == 50005000

quietly sum year
local ref_sum_year = r(sum)

quietly sum c_5
local ref_sum_c5 = r(sum)

di "Reference data created: _N=`=_N', sum(c_1)=50005000, sum(year)=`ref_sum_year', sum(c_5)=`ref_sum_c5'"


//	----------------------------------------------------------------------
//	Test 1: chunked save → directory use
//	----------------------------------------------------------------------
pq save "`tparquet'.parquet", replace chunk(1000)
pq use "`tparquet'.parquet/*.parquet", clear

assert_stats, n(10000) sum_c1(50005000) sum_year(`ref_sum_year') sum_c5(`ref_sum_c5') ///
	label("Test 1: chunked save + directory use")


//	----------------------------------------------------------------------
//	Test 2: chunked save with consolidate → single-file use
//	----------------------------------------------------------------------
pq save "`tparquet'.parquet", replace chunk(1000) consolidate
pq use "`tparquet'.parquet", clear

assert_stats, n(10000) sum_c1(50005000) sum_year(`ref_sum_year') sum_c5(`ref_sum_c5') ///
	label("Test 2: chunked save + consolidate")


//	----------------------------------------------------------------------
//	Test 3: stream + do_not_reload → data cleared → directory use
//	----------------------------------------------------------------------
pq save "`tparquet'.parquet", replace chunk(1000) stream do_not_reload

if _N != 0 {
	di as error "Test 3: expected 0 rows after stream do_not_reload, got `=_N'"
	exit 9
}
di as text "Test 3a: PASSED (data cleared after stream do_not_reload)"

pq use "`tparquet'.parquet/*.parquet", clear

assert_stats, n(10000) sum_c1(50005000) sum_year(`ref_sum_year') sum_c5(`ref_sum_c5') ///
	label("Test 3b: stream do_not_reload + directory use")


//	----------------------------------------------------------------------
//	Test 4: stream (without do_not_reload) → data reloaded after chunks
//	----------------------------------------------------------------------
//	Re-create reference data with the same seed so stats are identical.
set seed 20240101
create_data, n_rows(10000) n_cols(6)

pq save "`tparquet'.parquet", replace chunk(1000) stream

//	After stream without do_not_reload, dataset should be reloaded intact.
assert_stats, n(10000) sum_c1(50005000) sum_year(`ref_sum_year') sum_c5(`ref_sum_c5') ///
	label("Test 4: stream (reload) + data intact after write")


di as result "All tests PASSED"
