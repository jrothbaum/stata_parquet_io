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
	syntax, n(integer) sum_c1(real) sum_year(real) sum_c5(real) label(string)

	if _N != `n' {
		di as error "`label': expected `n' rows, got `=_N'"
		exit 9
	}

	quietly sum c_1
	assert abs(r(sum) - `sum_c1') < 0.5

	quietly sum year
	assert abs(r(sum) - `sum_year') < 0.5

	quietly sum c_5
	local tol = max(1, abs(`sum_c5')) * 1e-6
	assert abs(r(sum) - `sum_c5') < `tol'

	di as text "`label': PASSED (_N=`=_N')"
end

tempfile tcsv

set seed 20240101
create_data, n_rows(10000) n_cols(6)

assert _N == 10000
quietly sum c_1
local ref_sum_c1 = r(sum)
quietly sum year
local ref_sum_year = r(sum)
quietly sum c_5
local ref_sum_c5 = r(sum)

// Test 1: baseline CSV save/load
pq save_spss "`tcsv'.sav", replace
pq use_spss "`tcsv'.sav", clear
assert_stats, n(10000) sum_c1(`ref_sum_c1') sum_year(`ref_sum_year') sum_c5(`ref_sum_c5') ///
	label("Test 1: baseline CSV save/load")

// Test 2: parquet-only chunk option should fail on CSV
capture pq save_spss "`tcsv'.sav", replace chunk(1000)
assert _rc != 0

// Test 3: parquet-only chunk+consolidate should fail on CSV
capture pq save_spss "`tcsv'.sav", replace chunk(1000) consolidate
assert _rc != 0

// Test 4: parquet-only stream should fail on CSV
capture pq save_spss "`tcsv'.sav", replace chunk(1000) stream
assert _rc != 0

// Test 5: parquet-only stream+do_not_reload should fail on CSV
capture pq save_spss "`tcsv'.sav", replace chunk(1000) stream do_not_reload
assert _rc != 0

di as result "All CSV save_chunked mirror tests PASSED"


