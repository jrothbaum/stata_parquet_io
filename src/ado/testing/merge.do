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


set seed 20240101
create_data, n_rows(100) n_cols(10)

//	Save base parquet: c_1 (long, 1-100), c_2 (str "A"-"E"),
//	c_3 (str from rnormal), c_4 (int 0-99), c_5-c_10 (double)
tempfile tparquet
pq save "`tparquet'.parquet", replace

local ref_sum_c1 = 100 * 101 / 2		//	c_1 = _n, so sum = n*(n+1)/2 = 5050

//	Build merge file: widen c_2 to str100, stringify c_4, rename c_2-c_10 → c_12-c_20
recast str100 c_2
tostring c_4, replace
forvalues i = 2/10 {
	rename c_`i' c_`=`i'+10'
}
//	tparquet_merge.parquet: c_1 (key), c_12 (str100), c_13-c_14 (str), c_15-c_20 (double)
pq save "`tparquet'_merge.parquet", replace


//	----------------------------------------------------------------------
//	Test 1: merge 1:1 by c_1 (key merge — all 100 rows match)
//	----------------------------------------------------------------------
pq use "`tparquet'.parquet", clear
pq merge 1:1 c_1 using "`tparquet'_merge.parquet"

if _N != 100 {
	di as error "Test 1: expected 100 rows, got `=_N'"
	exit 9
}
quietly sum c_1
if r(sum) != `ref_sum_c1' {
	di as error "Test 1: c_1 sum `=r(sum)' != `ref_sum_c1'"
	exit 9
}
capture confirm variable c_12
if _rc {
	di as error "Test 1: c_12 not found in merged result"
	exit 9
}
quietly count if _merge == 3
if r(N) != 100 {
	di as error "Test 1: expected all 100 rows matched (_merge==3), got `=r(N)'"
	exit 9
}
di as text "Test 1 (merge 1:1 c_1): PASSED (_N=`=_N', all 100 matched)"
drop _merge


//	----------------------------------------------------------------------
//	Test 2: merge 1:1 _n with compress + compress_string_to_numeric
//	----------------------------------------------------------------------
pq use "`tparquet'.parquet", clear
pq merge 1:1 _n using "`tparquet'_merge.parquet", compress compress_string_to_numeric

if _N != 100 {
	di as error "Test 2: expected 100 rows, got `=_N'"
	exit 9
}
quietly sum c_1
if r(sum) != `ref_sum_c1' {
	di as error "Test 2: c_1 sum `=r(sum)' != `ref_sum_c1'"
	exit 9
}
capture confirm variable c_12
if _rc {
	di as error "Test 2: c_12 not found in merged result"
	exit 9
}
//	c_14 was tostring'd integers (0-99); compress_string_to_numeric should convert back
capture confirm numeric variable c_14
if _rc {
	di as error "Test 2: c_14 should be numeric after compress_string_to_numeric"
	exit 9
}
quietly count if _merge == 3
if r(N) != 100 {
	di as error "Test 2: expected all 100 rows matched, got `=r(N)'"
	exit 9
}
di as text "Test 2 (merge 1:1 _n, compress + compress_string_to_numeric): PASSED"
drop _merge


//	----------------------------------------------------------------------
//	Test 3: merge 1:1 _n with random_n(50)
//	random_n(50) loads 50 random rows from merge file; positional merge
//	gives 50 matched rows (_merge==3) and 50 master-only rows (_merge==1)
//	----------------------------------------------------------------------
pq use "`tparquet'.parquet", clear
pq merge 1:1 _n using "`tparquet'_merge.parquet", compress random_n(50)

if _N != 100 {
	di as error "Test 3: expected 100 rows, got `=_N'"
	exit 9
}
quietly sum c_1
if r(sum) != `ref_sum_c1' {
	di as error "Test 3: c_1 sum `=r(sum)' != `ref_sum_c1'"
	exit 9
}
capture confirm variable c_12
if _rc {
	di as error "Test 3: c_12 not found in merged result"
	exit 9
}
quietly count if _merge == 3
if r(N) != 50 {
	di as error "Test 3: expected 50 matched rows (_merge==3), got `=r(N)'"
	exit 9
}
quietly count if _merge == 1
if r(N) != 50 {
	di as error "Test 3: expected 50 master-only rows (_merge==1), got `=r(N)'"
	exit 9
}
di as text "Test 3 (merge 1:1 _n, random_n(50)): PASSED (50 matched, 50 master-only)"
drop _merge


//	----------------------------------------------------------------------
//	Test 4: merge 1:1 _n with in(1/90)
//	Loads rows 1-90 from merge file → 90 matched, 10 master-only
//	----------------------------------------------------------------------
pq use "`tparquet'.parquet", clear
pq merge 1:1 _n using "`tparquet'_merge.parquet", compress in(1/90)

if _N != 100 {
	di as error "Test 4: expected 100 rows, got `=_N'"
	exit 9
}
quietly sum c_1
if r(sum) != `ref_sum_c1' {
	di as error "Test 4: c_1 sum `=r(sum)' != `ref_sum_c1'"
	exit 9
}
capture confirm variable c_12
if _rc {
	di as error "Test 4: c_12 not found in merged result"
	exit 9
}
quietly count if _merge == 3
if r(N) != 90 {
	di as error "Test 4: expected 90 matched rows, got `=r(N)'"
	exit 9
}
quietly count if _merge == 1
if r(N) != 10 {
	di as error "Test 4: expected 10 master-only rows, got `=r(N)'"
	exit 9
}
di as text "Test 4 (merge 1:1 _n, in(1/90)): PASSED (90 matched, 10 master-only)"


capture erase "`tparquet'.parquet"
capture erase "`tparquet'_merge.parquet"

di as result "All merge tests PASSED"
