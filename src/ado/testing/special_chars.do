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
		quietly gen c_`cols_created' = 100 + floor(runiform()*60)
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_`ci' = rnormal()
		}
	}
end


set seed 20240101
create_data, n_rows(10000) n_cols(10)

tempfile tparquet

local special1 "Consultoria, Científico, Técnico"
local special2 "Consultoria, Científico, Tecnico__"


//	----------------------------------------------------------------------
//	Test 1: special/unicode characters survive a parquet round-trip
//	----------------------------------------------------------------------
replace c_2 = "`special1'" if _n <= 1000
pq save "`tparquet'.parquet", replace
clear

pq use "`tparquet'.parquet", clear

if _N != 10000 {
	di as error "Test 1: expected 10000 rows, got `=_N'"
	exit 9
}
//	Rows 1-1000 must have the special string
assert c_2[1] == "`special1'"
//	Rows 1001+ must retain their original single-character values (A-E)
assert length(c_2[1001]) == 1

di as text "Test 1 (special/unicode chars round-trip): PASSED"


//	----------------------------------------------------------------------
//	Test 2: overwrite with a slightly different special string
//	----------------------------------------------------------------------
replace c_2 = "`special2'" if _n <= 1000
pq save "`tparquet'.parquet", replace
clear

pq use "`tparquet'.parquet", clear

if _N != 10000 {
	di as error "Test 2: expected 10000 rows, got `=_N'"
	exit 9
}
assert c_2[1] == "`special2'"
assert length(c_2[1001]) == 1

di as text "Test 2 (overwrite with variant special string): PASSED"


capture erase "`tparquet'.parquet"

di as result "All special_chars tests PASSED"
