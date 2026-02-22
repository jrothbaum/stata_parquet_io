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
		quietly gen c_`cols_created' = floor(runiform()*4)		//	values 0, 1, 2, 3
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_`ci' = rnormal()
		}
	}
end


tempfile pq_label


//	----------------------------------------------------------------------
//	Test 1: save WITHOUT label → c_4 reloads as numeric
//	----------------------------------------------------------------------
set seed 20240101
create_data, n_rows(1000) n_cols(10)

quietly sum c_4
local ref_sum_c4 = r(sum)

pq save "`pq_label'.parquet", replace

pq use "`pq_label'.parquet", clear

if _N != 1000 {
	di as error "Test 1: expected 1000 rows, got `=_N'"
	exit 9
}
capture confirm numeric variable c_4
if _rc {
	di as error "Test 1: c_4 should be numeric when saved without label option"
	exit 9
}
quietly sum c_4
if abs(r(sum) - `ref_sum_c4') > 0.5 {
	di as error "Test 1: c_4 sum changed on reload (`=r(sum)' != `ref_sum_c4')"
	exit 9
}
di as text "Test 1 (save without label → numeric on reload): PASSED"


//	----------------------------------------------------------------------
//	Test 2: save WITH string value labels → c_4 reloads as label text
//	----------------------------------------------------------------------
set seed 20240101
create_data, n_rows(1000) n_cols(10)

label define lbl_str 0 "This" 1 "That" 2 "The Other" 3 "Anything", replace
label values c_4 lbl_str

pq save "`pq_label'.parquet", replace label

pq use "`pq_label'.parquet", clear

if _N != 1000 {
	di as error "Test 2: expected 1000 rows, got `=_N'"
	exit 9
}
capture confirm string variable c_4
if _rc {
	di as error "Test 2: c_4 should be string after pq save with label"
	exit 9
}
quietly tab c_4
if r(r) != 4 {
	di as error "Test 2: expected 4 distinct label values, got `=r(r)'"
	exit 9
}
quietly count if c_4 == "This" | c_4 == "That" | c_4 == "The Other" | c_4 == "Anything"
if r(N) != 1000 {
	di as error "Test 2: `=1000 - r(N)' rows have unexpected label values"
	exit 9
}
di as text "Test 2 (save with string labels → label text on reload): PASSED"


//	----------------------------------------------------------------------
//	Test 3: save WITH numeric-text value labels → c_4 reloads as string
//	label define 0 100 means value 0 maps to label text "100" etc.
//	----------------------------------------------------------------------
set seed 20240101
create_data, n_rows(1000) n_cols(10)

label define lbl_num 0 100 1 200 2 300 3 400, replace
label values c_4 lbl_num

pq save "`pq_label'.parquet", replace label

pq use "`pq_label'.parquet", clear

if _N != 1000 {
	di as error "Test 3: expected 1000 rows, got `=_N'"
	exit 9
}
capture confirm string variable c_4
if _rc {
	di as error "Test 3: c_4 should be string after pq save with numeric-text label"
	exit 9
}
quietly tab c_4
if r(r) != 4 {
	di as error "Test 3: expected 4 distinct label values, got `=r(r)'"
	exit 9
}
quietly count if c_4 == "100" | c_4 == "200" | c_4 == "300" | c_4 == "400"
if r(N) != 1000 {
	di as error "Test 3: `=1000 - r(N)' rows have unexpected label values"
	exit 9
}
di as text "Test 3 (save with numeric-text labels → string on reload): PASSED"


capture erase "`pq_label'.parquet"

di as result "All save_label tests PASSED"
